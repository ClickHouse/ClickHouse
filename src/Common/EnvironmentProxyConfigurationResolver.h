#pragma once

#include <Common/ProxyConfigurationResolver.h>

namespace DB
{

/*
 * Grabs proxy configuration from environment variables (http_proxy and https_proxy).
 * */
class EnvironmentProxyConfigurationResolver : public ProxyConfigurationResolver
{
public:
    ProxyConfiguration resolve(Protocol protocol) override;
    void errorReport(const ProxyConfiguration &) override {}
};

}
