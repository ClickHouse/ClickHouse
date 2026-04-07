#pragma once

#include "config.h"

#if USE_AWS_S3

#include <string_view>
#include <cstdint>

namespace DB::S3
{

/// Provider type defines the platform containing the object
/// we are trying to access
/// This information is useful for determining general support for
/// some feature like multipart copy which is currently supported by AWS
/// but not by GCS
enum class ProviderType : uint8_t
{
    AWS,
    GCS,
    UNKNOWN
};

std::string_view toString(ProviderType provider_type);

/// Mode in which we can use the XML API
/// This value can be same as the provider type but there can be a difference
/// For example, GCS can work in both
/// AWS compatible mode (accept headers starting with x-amz)
/// and GCS mode (accept only headers starting with x-goog)
/// Because GCS mode is enforced when some features are used we
/// need to have support for both.
enum class ApiMode : uint8_t
{
    AWS,
    GCS
};

std::string_view toString(ApiMode api_mode);

}

#endif
