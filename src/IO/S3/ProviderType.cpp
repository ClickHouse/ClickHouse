#include <IO/S3/ProviderType.h>

#if USE_AWS_S3

#include <string>

namespace DB::S3
{

std::string_view toString(ProviderType provider_type)
{
    using enum ProviderType;

    switch (provider_type)
    {
        case AWS:
            return "AWS";
        case GCS:
            return "GCS";
        case UNKNOWN:
            return "Unknown";
    }
}

std::string_view toString(ApiMode api_mode)
{
    using enum ApiMode;

    switch (api_mode)
    {
        case AWS:
            return "AWS";
        case GCS:
            return "GCS";
    }
}

}

#endif
