#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <IO/AzureBlobStorage/isRetryableAzureException.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <base/types.h>
#include <azure/core/credentials/credentials.hpp>

namespace DB
{

/// Run an Azure SDK operation under the IO retry policy: retry a transient RBAC-propagation 403 / other
/// retryable RequestFailedException (isRetryableAzureException) and a credential AuthenticationException
/// with bounded exponential backoff, then rethrow. Shared by the GetProperties, delete and copy callers.
template <typename Func>
auto retryAzureOperation(Func && func, size_t max_retries, const String & path, const LoggerPtr & log)
{
    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t i = 0;; ++i)
    {
        try
        {
            return func();
        }
        catch (const Azure::Core::RequestFailedException & e)
        {
            if (i + 1 >= max_retries || !isRetryableAzureException(e))
                throw;
            LOG_TEST(log, "Azure operation on {} failed at attempt {}, retrying: {}", path, i + 1, e.Message);
            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
        catch (const Azure::Core::Credentials::AuthenticationException & e)
        {
            if (i + 1 >= max_retries)
                throw;
            LOG_TEST(log, "Azure operation on {} failed at attempt {} (auth), retrying: {}", path, i + 1, e.what());
            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }
}

}

#endif
