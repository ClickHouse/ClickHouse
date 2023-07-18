#pragma once

#include <Common/ProxyConfiguration.h>

namespace DB
{

struct ProxyConfigurationResolver
{
    enum class Method
    {
        HTTP,
        HTTPS,
        ANY
    };

    virtual ~ProxyConfigurationResolver() = default;
    virtual ProxyConfiguration resolve(Method method) = 0;
    virtual void errorReport(const ProxyConfiguration & config) = 0;
};

}
