using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace queuesample
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("ConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine("ConnectionString is not found");
                Environment.Exit(1);  
            }
            
            var queueName = Environment.GetEnvironmentVariable("Queue");
            if (string.IsNullOrEmpty(queueName))
            {
                Console.WriteLine("Queue is not found");
                Environment.Exit(1);  
            }
            CloudStorageAccount account;
            if (CloudStorageAccount.TryParse(connectionString, out account) == false)
            {
                Console.WriteLine("ConnectionString is invalid");
                Environment.Exit(1);
            }

            var queue = account.CreateCloudQueueClient().GetQueueReference(queueName);

            queue.CreateIfNotExistsAsync().Wait();

            ExecuteUntilAsync(async() => await ExecuteAllAsync(queue), i=>TimeSpan.FromMinutes(1), 10).Wait();
        }
        static async Task ExecuteUntilAsync(Func<Task<bool>> action, Func<int, TimeSpan> backoff, int loopMax)
        {
            var count = 0;
            while(count < loopMax)
            {
                if (await action())
                {
                    count = 0;
                    continue;
                }
                await Task.Delay(backoff(++count));
            }
        }
        static async Task<bool> ExecuteAllAsync(CloudQueue queue)
        {
            await queue.FetchAttributesAsync();
            if (queue.ApproximateMessageCount == 0)
                return false;

            for(var i = 0; i < queue.ApproximateMessageCount; i++)
            {
                var message = await queue.GetMessageAsync();
                if (Execute(message))
                    await queue.DeleteMessageAsync(message);
            }
            return true;
        }
        static bool Execute(CloudQueueMessage message)
        {
            Console.WriteLine(message.AsString);
            return true;
        }
    }
}
