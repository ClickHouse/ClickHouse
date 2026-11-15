#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <IO/AzureBlobStorage/isRetryableAzureException.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AZURE_BLOB_STORAGE_ERROR;
}

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e)
{
    /// Always retry transport errors.
    if (dynamic_cast<const Azure::Core::Http::TransportException *>(&e))
        return true;

    /// Always retry 403 (RBAC-propagation lag); never treat it as data corruption.
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
        return true;

    /// 408 request-timeout is transient; retry it.
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::RequestTimeout)
        return true;

    /// Retry other 5xx errors just in case.
    return e.StatusCode >= Azure::Core::Http::HttpStatusCode::InternalServerError;
}

Int32 getAzureErrorCodeForLog(const Azure::Core::RequestFailedException & e)
{
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::None)
        return ErrorCodes::AZURE_BLOB_STORAGE_ERROR;
    return static_cast<Int32>(e.StatusCode);
}

}

#endif
