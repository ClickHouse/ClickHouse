#pragma once

#include <base/types.h>

#include <mutex>

#include <Common/ProxyConfigurationResolver.h>
#include <Poco/URI.h>

namespace DB
{

/*
 * Makes an HTTP GET request to the specified endpoint to obtain a proxy host.
 * */
class RemoteProxyConfigurationResolver : public ProxyConfigurationResolver
{
public:
    RemoteProxyConfigurationResolver(
        const Poco::URI & endpoint_,
        String proxy_protocol_,
        unsigned proxy_port_,
        unsigned cache_ttl_
    );

    ProxyConfiguration resolve() override;

    void errorReport(const ProxyConfiguration & config) override;

private:

    /// Endpoint to obtain a proxy host.
    const Poco::URI endpoint;
    /// Scheme for obtained proxy.
    const String proxy_protocol;
    /// Port for obtained proxy.
    const unsigned proxy_port;

    std::mutex cache_mutex;
    bool cache_valid = false;
    std::chrono::time_point<std::chrono::system_clock> cache_timestamp;
    const std::chrono::seconds cache_ttl{0};
    ProxyConfiguration cached_config;
};

}
