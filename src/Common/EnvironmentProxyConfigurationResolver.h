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
    EnvironmentProxyConfigurationResolver(Protocol request_protocol, ConnectProtocolPolicy connect_protocol_policy_);

    ProxyConfiguration resolve() override;
    void errorReport(const ProxyConfiguration &) override {}
};

}
