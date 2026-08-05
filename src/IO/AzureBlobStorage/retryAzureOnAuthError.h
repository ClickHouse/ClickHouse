#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <base/types.h>
#include <azure/core/credentials/credentials.hpp>
#include <azure/storage/blobs/blob_client.hpp>

namespace DB
{

/// A credential AuthenticationException is thrown by the token layer around the transport, so the SDK
/// RetryPolicy (which retries transient HTTP responses and transport errors) never sees it; retry it
/// here with bounded exponential backoff for call sites without a ClickHouse-level retry loop.
template <typename Func>
auto retryAzureOnAuthError(Func && func, size_t max_retries, const String & path, const LoggerPtr & log)
{
    size_t sleep_time_with_backoff_milliseconds = 100;
    for (size_t i = 0;; ++i)
    {
        try
        {
            return func();
        }
        catch (const Azure::Core::Credentials::AuthenticationException & e)
        {
            if (i + 1 >= max_retries)
                throw;
            LOG_DEBUG(log, "Azure operation on {} failed at attempt {}/{} (auth), retrying: {}", path, i + 1, max_retries, e.what());
            sleepForMilliseconds(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }
}

/// GetProperties() issued outside the IO retry loops (existence/metadata/size probes, upload verification).
inline Azure::Storage::Blobs::Models::BlobProperties getBlobPropertiesWithRetry(
    const Azure::Storage::Blobs::BlobClient & client, size_t max_retries, const String & path, const LoggerPtr & log)
{
    return retryAzureOnAuthError([&] { return client.GetProperties().Value; }, max_retries, path, log);
}

}

#endif
