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

    static bool useTunneling(Protocol request_protocol, Protocol proxy_protocol, bool disable_tunneling_for_https_requests_over_http_proxy)
    {
        bool is_https_request_over_http_proxy = request_protocol == Protocol::HTTPS && proxy_protocol == Protocol::HTTP;
        return is_https_request_over_http_proxy && !disable_tunneling_for_https_requests_over_http_proxy;
    }

    Protocol request_protocol;
    bool disable_tunneling_for_https_requests_over_http_proxy = false;
};

}
