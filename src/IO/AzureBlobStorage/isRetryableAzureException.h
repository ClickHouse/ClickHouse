#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <azure/core/http/http.hpp>

namespace DB
{

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e, bool may_be_provisioning_access = false);

/// True for HTTP 401/403, which indicate the SAS token (or other credentials) is rejected or expired.
bool isAzureAccessTokenExpiredError(const Azure::Core::RequestFailedException & e);

}

#endif
