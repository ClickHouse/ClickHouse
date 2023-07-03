#pragma once

#include <IO/S3/ProxyConfiguration.h>

namespace DB::S3
{

class ProxyConfigurationProvider
{
public:
    static std::shared_ptr<ProxyConfiguration> get(
        const String & prefix, const Poco::Util::AbstractConfiguration & proxy_resolver_config);
};

}
