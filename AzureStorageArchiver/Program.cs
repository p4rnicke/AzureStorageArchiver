using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;

namespace AzureStorageArchiver
{
    class Program
    {
        static void Main(string[] args)
        {
            var blobClient = GetBlobClient(ConfigurationManager.AppSettings["AzureStorage"]);
            if (blobClient != null)
            {
                var argList = args.ToList();
                string containerFilter = null;
                if (argList.Count >= 1) containerFilter = argList[0];
                string directoryFilter = null;
                if (argList.Count == 2) directoryFilter = argList[1];
                var containers = blobClient.ListContainers();

                foreach (CloudBlobContainer container in containers)
                {
                    if (containerFilter == null || container.Name == containerFilter)
                    {
                        Console.WriteLine($"Processing /{container.Name} container");
                        ProcessBlobDirectory(container.ListBlobs(), StandardBlobTier.Archive, directoryFilter);
                    }
                    else
                    {
                        Console.WriteLine($"Skipping /{container.Name} container");
                    }
                }
            }
        }

        static void ProcessBlobDirectory(IEnumerable<IListBlobItem> list, StandardBlobTier tier, string directoryFilter)
        {
            bool hadNewLine = false;
            foreach (var item in list)
            {
                var type = item.GetType();
                if (type == typeof(CloudBlobDirectory))
                {
                    var directory = (CloudBlobDirectory)item;
                    if (directoryFilter == null || directory.Prefix == $"{directoryFilter}/")
                    {
                        Console.WriteLine($"Processing {directory.Uri.AbsolutePath} directory");
                        ProcessBlobDirectory(directory.ListBlobs(), tier, null);
                    }
                    else
                    {
                        Console.WriteLine($"Skipping {directory.Uri.AbsolutePath} directory");
                    }
                    hadNewLine = true;
                }
                else if (type == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)item;
                    var blobTier = blob.Properties.StandardBlobTier;
                    if (blobTier != tier)
                    {
                        Console.WriteLine($"Updating {blob.Name} blob tier to {tier}");
                        blob.SetStandardBlobTier(tier);
                        hadNewLine = true;
                    }
                    else
                    {
                        Console.Write($".");
                        hadNewLine = false;
                    }
                }
                else
                {
                    Console.WriteLine($"Unknown item type {type} for {item.Uri}");
                    hadNewLine = true;
                }
            }
            if (!hadNewLine) Console.WriteLine($"Done");
        }

        /// <summary>
        /// Get the reference to the blob client
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <returns></returns>
        static CloudBlobClient GetBlobClient(string connectionString)
        {
            // Retrieve blob storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            // Disable Nagle and Expect100
            ServicePoint tableServicePoint = ServicePointManager.FindServicePoint(storageAccount.BlobEndpoint);
            tableServicePoint.UseNagleAlgorithm = false;
            tableServicePoint.Expect100Continue = false;
            tableServicePoint.ConnectionLimit = 1000;
            // Create the blob client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            // Set retry policy
            blobClient.DefaultRequestOptions.RetryPolicy = new ExponentialRetry(TimeSpan.FromMilliseconds(250), 5);
            return blobClient;
        }

    }
}
