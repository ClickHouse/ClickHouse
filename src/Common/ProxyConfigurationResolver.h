#pragma once

#include <Common/ProxyConfiguration.h>

namespace DB
{

struct ProxyConfigurationResolver
{
    using Protocol = ProxyConfiguration::Protocol;

    explicit ProxyConfigurationResolver(Protocol request_protocol_, bool disable_tunneling_for_https_requests_over_http_proxy_ = false)
        : request_protocol(request_protocol_), disable_tunneling_for_https_requests_over_http_proxy(disable_tunneling_for_https_requests_over_http_proxy_)
    {
    }

    virtual ~ProxyConfigurationResolver() = default;
    virtual ProxyConfiguration resolve() = 0;
    virtual void errorReport(const ProxyConfiguration & config) = 0;

protected:
    Protocol request_protocol;
    bool disable_tunneling_for_https_requests_over_http_proxy = false;
};

}
