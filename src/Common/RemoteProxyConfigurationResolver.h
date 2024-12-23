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

    struct RemoteServerConfiguration
    {
        Poco::URI endpoint;
        String proxy_protocol;
        unsigned proxy_port;
        unsigned cache_ttl_;
    };

    RemoteProxyConfigurationResolver(
        const RemoteServerConfiguration & remote_server_configuration_,
        Protocol request_protocol_,
        bool disable_tunneling_for_https_requests_over_http_proxy_ = true);

    ProxyConfiguration resolve() override;

    void errorReport(const ProxyConfiguration & config) override;

private:
    RemoteServerConfiguration remote_server_configuration;

    std::mutex cache_mutex;
    bool cache_valid = false;
    std::chrono::time_point<std::chrono::system_clock> cache_timestamp;
    const std::chrono::seconds cache_ttl{0};
    ProxyConfiguration cached_config;
};

}
