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

bool supportsMultiPartCopy(ProviderType provider_type)
{
    return provider_type != ProviderType::GCS;
}

ProviderType getProviderTypeFromURL(const std::string & url)
{
    if (url.find(".amazonaws.com") != std::string::npos)
        return ProviderType::AWS;

    if (url.find("storage.googleapis.com") != std::string::npos)
        return ProviderType::GCS;

    return ProviderType::UNKNOWN;
}

}

#endif
