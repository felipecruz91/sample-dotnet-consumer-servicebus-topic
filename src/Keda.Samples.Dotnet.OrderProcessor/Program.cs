using System;
using System.Runtime.Loader;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace Keda.Samples.Dotnet.OrderProcessor
{
    internal class Program
    {
        private static ISubscriptionClient _subscriptionClient;

        private static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }


        private static async Task MainAsync()
        {
            var connectionString = GetConnectionString();
            const string topicPath = "orders";
            const string subscriptionName = "sbtopic-sub1";

            _subscriptionClient = new SubscriptionClient(connectionString, topicPath, subscriptionName);

            RegisterOnMessageHandlerAndReceiveMessages();

            PreventConsoleAppToExit();
        }

        private static string GetConnectionString()
        {
            var kedaConnectionString = Environment.GetEnvironmentVariable("KEDA_SERVICEBUS_TOPIC_CONNECTIONSTRING");

            var match = Regex.Match(kedaConnectionString, @"(?<Endpoint>.*);EntityPath=.*", RegexOptions.Compiled);
            if (!match.Success)
            {
                throw new Exception("Connection string does not contain ';EntityPath=' segment at the end of it.");
            }

            var connectionString = match.Groups["Endpoint"].Value;

            Console.WriteLine($"Connection string: {connectionString}\n");

            return connectionString;
        }

        private static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = true
            };

            // Register the function that processes messages.
            _subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        private static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message.
            Console.WriteLine(
                $"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");

            // Simulate some processing
            await Task.Delay(2000).ConfigureAwait(false);
        }

        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }

        private static void PreventConsoleAppToExit()
        {
            var ended = new ManualResetEventSlim();
            var starting = new ManualResetEventSlim();

            AssemblyLoadContext.Default.Unloading += ctx =>
            {
                Console.WriteLine("Unloding fired");
                starting.Set();
                Console.WriteLine("Waiting for completion");
                ended.Wait();
            };

            Console.WriteLine("Waiting for signals");
            starting.Wait();

            Console.WriteLine("Received signal gracefully shutting down");
            _subscriptionClient.CloseAsync();
            Thread.Sleep(5000);
            ended.Set();
        }
    }
}