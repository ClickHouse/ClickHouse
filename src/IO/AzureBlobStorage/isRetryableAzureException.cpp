#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <IO/AzureBlobStorage/isRetryableAzureException.h>

namespace DB
{

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e, bool may_be_provisioning_access)
{
    /// Always retry transport errors.
    if (dynamic_cast<const Azure::Core::Http::TransportException *>(&e))
        return true;

    /// Azure may be provisioning access for quite a long time, so 403 is retriable in some cases
    /// It makes sense for IDisk::checkAccess() which is called early on startup and terminates the server/keeper if the check fails
    if (may_be_provisioning_access && e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
        return true;

    /// Retry other 5xx errors just in case.
    return e.StatusCode >= Azure::Core::Http::HttpStatusCode::InternalServerError;
}

}

#endif
