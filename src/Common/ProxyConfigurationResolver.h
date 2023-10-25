#pragma once

#include <Common/ProxyConfiguration.h>

namespace DB
{

struct ProxyConfigurationResolver
{
    using Protocol = ProxyConfiguration::Protocol;

    explicit ProxyConfigurationResolver(Protocol request_protocol_, bool use_tunneling_for_https_requests_over_http_proxy_ = true)
        : request_protocol(request_protocol_), use_tunneling_for_https_requests_over_http_proxy(use_tunneling_for_https_requests_over_http_proxy_)
    {
    }

    virtual ~ProxyConfigurationResolver() = default;
    virtual ProxyConfiguration resolve() = 0;
    virtual void errorReport(const ProxyConfiguration & config) = 0;

protected:
    Protocol request_protocol;
    bool use_tunneling_for_https_requests_over_http_proxy;
};

}
