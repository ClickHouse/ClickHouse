#include <IO/AzureBlobStorage/getBlobPropertiesWithRetry.h>

#if USE_AZURE_BLOB_STORAGE

#include <IO/AzureBlobStorage/retryAzureOperation.h>

namespace DB
{

Azure::Storage::Blobs::Models::BlobProperties getBlobPropertiesWithRetry(
    const Azure::Storage::Blobs::BlobClient & client, size_t max_retries, const String & path, const LoggerPtr & log)
{
    return retryAzureOperation([&] { return client.GetProperties().Value; }, max_retries, path, log);
}

}

#endif
