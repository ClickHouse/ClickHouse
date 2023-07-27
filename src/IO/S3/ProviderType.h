#pragma once

#include "config.h"

#if USE_AWS_S3

#include <string_view>
#include <cstdint>

namespace DB::S3
{

enum class ProviderType : uint8_t
{
    AWS,
    GCS,
    UNKNOWN
};

std::string_view toString(ProviderType provider_type);

bool supportsMultiPartCopy(ProviderType provider_type);

ProviderType getProviderTypeFromURL(const std::string & url);

}

#endif
