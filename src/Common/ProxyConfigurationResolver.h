#pragma once

#include <Common/ProxyConfiguration.h>

namespace DB
{

struct ProxyConfigurationResolver
{
    virtual ~ProxyConfigurationResolver() = default;
    virtual ProxyConfiguration resolve(bool https) = 0;
    virtual void errorReport(const ProxyConfiguration & config) = 0;
};

}
