#pragma once

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/ProxyConfiguration.h>

namespace DB::S3
{

class ProxyConfigurationProvider
{
public:
    static std::shared_ptr<ProxyConfiguration> get(const String & config_prefix, const Poco::Util::AbstractConfiguration & configuration);

    static std::shared_ptr<ProxyConfiguration> get(const String & protocol);
};

}

#endif
