#pragma once

#include <base/types.h>

#include <Common/ProxyConfigurationResolver.h>
#include <Poco/URI.h>

namespace DB
{

class RemoteProxyConfigurationResolver : public ProxyConfigurationResolver
{
public:
    RemoteProxyConfigurationResolver(
        const Poco::URI & endpoint_,
        String proxy_scheme_,
        unsigned proxy_port_,
        unsigned cache_ttl_
    );

    std::optional<ProxyConfiguration> resolve(bool https) override;

    void errorReport(const ProxyConfiguration & config) override;

private:

    /// Endpoint to obtain a proxy host.
    const Poco::URI endpoint;
    /// Scheme for obtained proxy.
    const String proxy_scheme;
    /// Port for obtained proxy.
    const unsigned proxy_port;

    std::mutex cache_mutex;
    bool cache_valid = false;
    std::chrono::time_point<std::chrono::system_clock> cache_timestamp;
    const std::chrono::seconds cache_ttl{0};
    ProxyConfiguration cached_config;
};

}

