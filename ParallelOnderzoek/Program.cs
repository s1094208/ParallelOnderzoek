using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace ParallelOnderzoek
{
	public class Program
	{
		public static int currentQueryIndex { get; set; }
		public static string[] ReadQueries()
		{
			string path = "queries2.txt";

			string readText = File.ReadAllText(path);
			string[] queries = readText.Split(';');

			return queries;
		}

		public static void ExecuteQuery(string query)
		{
			var cfg = InitOptions<AppConfig>();

			//your connection string
			string connString = @"Data Source=" + cfg.Connection.DataSource + ";Initial Catalog="
				+ cfg.Connection.Database + ";Persist Security Info=True;User ID=" + cfg.Connection.Username + ";Password=" + cfg.Connection.Password;

			//create instance of database connection
			SqlConnection conn = new SqlConnection(connString);

			try
			{

				//open connection
				conn.Open();

				DataTable dataTable = new DataTable();

				using (SqlCommand command = new SqlCommand(query, conn))
				{
					SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
					dataAdapter.Fill(dataTable);
					dataAdapter.Dispose();
				}

				var columnNames = new List<string>();

				foreach(DataColumn column in dataTable.Columns)
				{
					columnNames.Add(column.ColumnName);
				}

				Console.WriteLine(columnNames.Count);

				conn.Close();
			}
			catch (Exception e)
			{
				Console.WriteLine("Error: " + e.Message);
			}
		}

		public static void ExecuteParallel(int amountParallel)
		{
			string[] queries = ReadQueries();

			Parallel.For(0, amountParallel,
				number =>
				{
					currentQueryIndex++;
					if (currentQueryIndex > queries.Length-1)
					{
						currentQueryIndex = 0;
					}
					Console.WriteLine("De huidige query is " + currentQueryIndex);
					ExecuteQuery(queries[currentQueryIndex]);
				});
		}

		public static void ExecuteOther(int amountParallel)
		{
			string[] queries = ReadQueries();

			var myExecutionBlock = new ActionBlock<int>(queryIndex =>
			{
				ExecuteQuery(queries[queryIndex]);
			}, new ExecutionDataflowBlockOptions
			{
				MaxDegreeOfParallelism = amountParallel
			});

			int amountIndex = 0;

			while(amountIndex < amountParallel)
			{
				currentQueryIndex++;
				if (currentQueryIndex > queries.Length - 1)
				{
					currentQueryIndex = 0;
				}
				myExecutionBlock.Post(currentQueryIndex);
				amountIndex++;
			}

			myExecutionBlock.Complete();
			myExecutionBlock.Completion.Wait();

			Console.WriteLine("Finised");
		}

		public static void ExecuteEverything(int amountOfQueries, int amountParallel)
		{
			string path = "test.txt";

			currentQueryIndex = -1;

			Stopwatch stopWatch = new Stopwatch();
			stopWatch.Start();

			for (int i = 0; i < amountOfQueries; i++)
			{
				ExecuteParallel(amountParallel);
			}

			stopWatch.Stop();
			TimeSpan ts = stopWatch.Elapsed;

			string elapsedTime = String.Format("{0:00}:{1:00},{2:00}",
			ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
			Console.WriteLine("RunTime " + elapsedTime);

			string writeTime = $"{elapsedTime}\t";

			File.AppendAllText(path, writeTime);

		}

		public static List<int> CalculateAmountParallel(int amountOfQueries)
		{
			var parallelValueList = new List<int>();
			parallelValueList.Add(1);

			int amountToDevide = amountOfQueries;

			while(amountToDevide > 1)
			{
				amountToDevide /= 2;
				parallelValueList.Add(amountOfQueries / amountToDevide);
				Console.WriteLine(amountToDevide + " sequentieel en " + amountOfQueries/amountToDevide + "parallel");
			}

			return parallelValueList;
		}

		public static void Main(string[] args)
		{
			try
			{
				Console.Write("Aantal queries: ");
				var input = Console.ReadLine();
				int amount;

				if(Int32.TryParse(input, out amount))
				{
					Console.WriteLine(amount + " queries");

					List<int> parallelValues = CalculateAmountParallel(amount);

					foreach (int parallelValue in parallelValues)
					{
						ExecuteEverything(amount / parallelValue, parallelValue);
					}

					Console.WriteLine("Done!");
				}

			}
			catch (Exception e)
			{
				Console.WriteLine("Error: " + e.Message);
			}

			Console.Read();
		}

		private static T InitOptions<T>()
			where T : new()
		{
			var config = InitConfig();
			return config.Get<T>();
		}

		private static IConfigurationRoot InitConfig()
		{
			var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
			var builder = new ConfigurationBuilder()
				.AddJsonFile($"appsettings.json", true, true)
				.AddJsonFile($"appsettings.{env}.json", true, true)
				.AddEnvironmentVariables();

			return builder.Build();
		}
	}
}
