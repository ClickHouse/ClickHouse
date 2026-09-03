#pragma once

#include <atomic>
#include <base/types.h>

#include <Common/ProxyConfigurationResolver.h>
#include <Poco/URI.h>

namespace DB
{

/*
 * Round-robin proxy list resolver.
 * */
class ProxyListConfigurationResolver : public ProxyConfigurationResolver
{
public:
    ProxyListConfigurationResolver(
        std::vector<Poco::URI> proxies_,
        Protocol request_protocol_,
        const std::string & no_proxy_hosts_,
        bool disable_tunneling_for_https_requests_over_http_proxy_ = false);

    ProxyConfiguration resolve() override;

    void errorReport(const ProxyConfiguration &) override {}

private:
    std::vector<Poco::URI> proxies;
    std::string no_proxy_hosts;

    /// Access counter to get proxy using round-robin strategy.
    std::atomic<size_t> access_counter;

};

}
