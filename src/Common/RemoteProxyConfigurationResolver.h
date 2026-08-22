#pragma once

#include <base/types.h>

#include <mutex>

#include <Common/ProxyConfigurationResolver.h>
#include <Poco/URI.h>

namespace DB
{

struct ConnectionTimeouts;

struct RemoteProxyHostFetcher
{
    virtual ~RemoteProxyHostFetcher() = default;
    virtual std::string fetch(const Poco::URI & endpoint, const ConnectionTimeouts & timeouts) = 0;
};

struct RemoteProxyHostFetcherImpl : public RemoteProxyHostFetcher
{
    std::string fetch(const Poco::URI & endpoint, const ConnectionTimeouts & timeouts) override;
};

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
        const std::chrono::seconds cache_ttl_;
    };

    RemoteProxyConfigurationResolver(
        const RemoteServerConfiguration & remote_server_configuration_,
        Protocol request_protocol_,
        const std::string & no_proxy_hosts_,
        std::shared_ptr<RemoteProxyHostFetcher> fetcher_,
        bool disable_tunneling_for_https_requests_over_http_proxy_ = false);

    ProxyConfiguration resolve() override;

    void errorReport(const ProxyConfiguration & config) override;

private:
    RemoteServerConfiguration remote_server_configuration;
    std::string no_proxy_hosts;
    std::shared_ptr<RemoteProxyHostFetcher> fetcher;

    std::mutex cache_mutex;
    bool cache_valid = false;
    std::chrono::time_point<std::chrono::system_clock> cache_timestamp;
    ProxyConfiguration cached_config;
};

}
