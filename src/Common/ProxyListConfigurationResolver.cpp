#include <Common/ProxyListConfigurationResolver.h>

#include <Common/StringUtils.h>
#include <Poco/URI.h>

namespace DB
{

ProxyListConfigurationResolver::ProxyListConfigurationResolver(
    std::vector<Poco::URI> proxies_,
    Protocol request_protocol_,
    const std::string & no_proxy_hosts_,
    bool disable_tunneling_for_https_requests_over_http_proxy_)
    : ProxyConfigurationResolver(request_protocol_, disable_tunneling_for_https_requests_over_http_proxy_),
    proxies(std::move(proxies_)), no_proxy_hosts(no_proxy_hosts_)
{
}

ProxyConfiguration ProxyListConfigurationResolver::resolve()
{
    if (proxies.empty())
    {
        return {};
    }

    /// Avoid atomic increment if number of proxies is 1.
    size_t index = proxies.size() > 1 ? (access_counter++) % proxies.size() : 0;

    auto & proxy = proxies[index];

    bool use_tunneling_for_https_requests_over_http_proxy = ProxyConfiguration::useTunneling(
        request_protocol,
        ProxyConfiguration::protocolFromString(proxy.getScheme()),
        disable_tunneling_for_https_requests_over_http_proxy);

    return ProxyConfiguration {
        proxy.getHost(),
        ProxyConfiguration::protocolFromString(proxy.getScheme()),
        proxy.getPort(),
        use_tunneling_for_https_requests_over_http_proxy,
        request_protocol,
        no_proxy_hosts
    };
}

}
