#include <Common/ProxyListConfigurationResolver.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <Poco/URI.h>

namespace DB
{

ProxyListConfigurationResolver::ProxyListConfigurationResolver(std::vector<Poco::URI> http_proxies_, std::vector<Poco::URI> https_proxies_)
    : http_proxies(http_proxies_), https_proxies(https_proxies_), any_proxies({})
{
}

ProxyConfiguration ProxyListConfigurationResolver::resolve(Method method)
{
    const auto proxy_uri = getProxyURI(method);

    LOG_DEBUG(&Poco::Logger::get("ProxyListConfigurationResolver"), "Use proxy: {}", proxy_uri.toString());

    return ProxyConfiguration {proxy_uri.getHost(), proxy_uri.getScheme(), proxy_uri.getPort()};
}

Poco::URI ProxyListConfigurationResolver::getProxyURI(Method method)
{
    switch (method)
    {
        case Method::HTTP:
            return http_proxies.get();
        case Method::HTTPS:
            return https_proxies.get();
        case Method::ANY:
            return any_proxies.get();
    }
}

}
