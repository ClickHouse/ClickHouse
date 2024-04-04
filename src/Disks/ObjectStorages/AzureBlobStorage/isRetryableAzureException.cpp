#include <Disks/ObjectStorages/AzureBlobStorage/isRetryableAzureException.h>
#if USE_AZURE_BLOB_STORAGE

namespace DB
{

bool isRetryableAzureRequestException(const Azure::Core::RequestFailedException & e)
{
    /// Always retry transport errors.
    if (dynamic_cast<const Azure::Core::Http::TransportException *>(&e))
        return true;

    /// Retry other 5xx errors just in case.
    return e.StatusCode >= Azure::Core::Http::HttpStatusCode::InternalServerError;
}

#endif

}
