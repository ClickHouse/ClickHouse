#include <IO/AzureBlobStorage/getBlobPropertiesWithRetry.h>

#if USE_AZURE_BLOB_STORAGE

#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <azure/core/credentials/credentials.hpp>

namespace DB
{

Azure::Storage::Blobs::Models::BlobProperties getBlobPropertiesWithRetry(
    const Azure::Storage::Blobs::BlobClient & client, size_t max_retries, const String & path, const LoggerPtr & log)
{
    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t i = 0;; ++i)
    {
        try
        {
            return client.GetProperties().Value;
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (i + 1 >= max_retries || !isRetryableAzureException(e))
                throw;
            LOG_TEST(log, "GetProperties for {} failed at attempt {}, retrying: {}", path, i + 1, e.Message);
            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
        catch (const Azure::Core::Credentials::AuthenticationException & e)
        {
            /// Credential/RBAC token-acquisition failure is transient (same window as 403); retry
            /// within the same budget.
            if (i + 1 >= max_retries)
                throw;
            LOG_TEST(log, "GetProperties for {} failed at attempt {} (auth), retrying: {}", path, i + 1, e.what());
            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }
}

}

#endif
