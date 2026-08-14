#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <azure/core/http/http.hpp>

namespace DB
{

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e);

}

#endif
