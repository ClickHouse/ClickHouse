#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <base/types.h>
#include <Common/Logger.h>
#include <IO/AzureBlobStorage/retryAzureOperation.h>
#include <azure/storage/blobs/blob_client.hpp>

namespace DB
{

/// GetProperties() issued outside the IO retry loops would let a transient Azure 403 (RBAC-propagation
/// window) or a credential AuthenticationException escape raw; retry it with the same policy the IO
/// loops use (isRetryableAzureException + bounded exponential backoff).
inline Azure::Storage::Blobs::Models::BlobProperties getBlobPropertiesWithRetry(
    const Azure::Storage::Blobs::BlobClient & client, size_t max_retries, const String & path, const LoggerPtr & log)
{
    return retryAzureOperation([&] { return client.GetProperties().Value; }, max_retries, path, log);
}

}

#endif
