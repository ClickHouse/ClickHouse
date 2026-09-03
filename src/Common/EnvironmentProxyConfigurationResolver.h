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
    explicit EnvironmentProxyConfigurationResolver(Protocol request_protocol, bool disable_tunneling_for_https_requests_over_http_proxy_ = false);

    ProxyConfiguration resolve() override;
    void errorReport(const ProxyConfiguration &) override {}
};

}
