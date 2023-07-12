#pragma once

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/ProxyConfiguration.h>

namespace DB::S3
{

class ProxyConfigurationProvider
{
public:
    static std::shared_ptr<ProxyConfiguration> get(const String & config_prefix);

    static std::shared_ptr<ProxyConfiguration> get();
};

}

#endif
