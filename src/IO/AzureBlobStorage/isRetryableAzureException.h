#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <azure/core/http/http.hpp>
#include <base/types.h>

namespace DB
{

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e);

/// `HttpStatusCode::None` (a transport error) is 0, which `system.blob_storage_log` reserves for success.
Int32 getAzureErrorCodeForLog(const Azure::Core::RequestFailedException & e);

}

#endif
