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

    /// 403 is always retryable so a transient RBAC-propagation 403 isn't misreported as POTENTIALLY_BROKEN_DATA_PART.
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
        return true;

    /// A request timeout (incl. a synthetic 408 built from a transport timeout) is transient; retry it (#110724).
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::RequestTimeout)
        return true;

    /// Retry other 5xx errors just in case.
    return e.StatusCode >= Azure::Core::Http::HttpStatusCode::InternalServerError;
}

}

#endif
