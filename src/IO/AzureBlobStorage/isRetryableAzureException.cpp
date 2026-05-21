#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <IO/AzureBlobStorage/isRetryableAzureException.h>

namespace DB
{

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e)
{
    /// Always retry transport errors.
    if (dynamic_cast<const Azure::Core::Http::TransportException *>(&e))
        return true;

    /// Azure may be provisioning access for quite a long time, so 403 is always treated as retryable.
    /// Without this, a brief permission-propagation window after a credential/role change causes
    /// in-flight SELECTs to fail and, worse, to be reclassified as POTENTIALLY_BROKEN_DATA_PART by
    /// MergeTreeSequentialSource -> StorageSharedMergeTree::reportBrokenPart, which triggers loud
    /// alerting via ForcedCriticalErrorsLogger even though the underlying part is fine.
    /// A genuinely permanent 403 still surfaces: in-buffer retries are bounded, and the final
    /// failure is reported as a plain Azure error rather than a phantom broken part.
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
        return true;

    /// Retry other 5xx errors just in case.
    return e.StatusCode >= Azure::Core::Http::HttpStatusCode::InternalServerError;
}

}

#endif
