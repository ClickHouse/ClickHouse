#pragma once

#include <Common/ProxyConfiguration.h>

namespace DB
{

struct ProxyConfigurationResolver
{
    using Protocol = ProxyConfiguration::Protocol;

    virtual ~ProxyConfigurationResolver() = default;
    virtual ProxyConfiguration resolve() = 0;
    virtual void errorReport(const ProxyConfiguration & config) = 0;
};

}
