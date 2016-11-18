using System;
using System.Globalization;
using System.Threading;
using StackExchange.Redis;

namespace RedisWatcher
{
    public class Program
    {
        private static int _correlation = 0;
        private static ConnectionMultiplexer _connection;

        public static void Main(string[] args)
        {
            var connectionString = GetConnectionString(args);
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine("Please provide connection string in the `REDIS_CONNECTIONSTRING` environment variable or as the first parameter");
                Console.WriteLine("Exiting");
                return;
            }

            _connection = SubscribeToEvents(connectionString);
            using (var timer = new Timer(OnTick, connectionString, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(15)))
            {
                Console.ReadLine();
            }
        }

        private static string GetConnectionString(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTIONSTRING");
            if (string.IsNullOrWhiteSpace(connectionString) && args.Length > 0)
            {
                connectionString = args[0];
            }

            return connectionString;
        }

        private static void OnTick(object connectionString)
        {
            int correlationId = Interlocked.Increment(ref _correlation);
            try
            {
                Log("Connecting to Redis.", correlationId);
                using (var redis = ConnectionMultiplexer.Connect((string)connectionString))
                {
                    Log("Connected to Redis.", correlationId);
                    var db = redis.GetDatabase();
                    Log("Executing script.", correlationId);
                    var result = db.ScriptEvaluate("return 42");
                    Log($"Script evaluated successfully. Result: `{(string)result}`.", correlationId);
                    redis.Close();
                    Log("Connection to Redis closed.", correlationId);
                }
            }
            catch (Exception ex)
            {
                Log($"Exception thrown: {ex}", correlationId);
            }
        }

        private static ConnectionMultiplexer SubscribeToEvents(string connectionString)
        {
            ConnectionMultiplexer connection;

            retry:
            try
            {
                Log($"Opening connection.", 0);
                connection = ConnectionMultiplexer.Connect(connectionString);
                Log($"Connection opened successfully.", 0);
            }
            catch (Exception ex)
            {
                Log($"Failed opening connection: {ex}", 0);
                Thread.Sleep(TimeSpan.FromSeconds(15));
                goto retry;
            }

            Log($"Subscribing to events.", 0);

            connection.ConnectionFailed +=
                (s, e) => Log($"Connection failed. Connection type: {e.ConnectionType }, Failure type {e.FailureType}, Exception: {e.Exception}", 0);

            connection.ConnectionRestored +=
                (s, e) => Log($@"Connection restored. IsConnected: {connection.IsConnected}, Connection type: {e.ConnectionType}, Failure type {e.FailureType}, Exception: {e.Exception}", 0);

            Log($"Subscribed to events.", 0);

            return connection;
        }

        private static void Log(string message, int correlationId)
        {
            Console.WriteLine(
                $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)} [{correlationId:D10}] {message}");
        }
    }
}
