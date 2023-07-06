#pragma once

#include <Common/ProxyConfigurationResolver.h>

namespace DB
{

class EnvironmentProxyConfigurationResolver : public ProxyConfigurationResolver
{
public:
    ProxyConfiguration resolve(bool https) override;
    void errorReport(const ProxyConfiguration &) override {}
};

}
