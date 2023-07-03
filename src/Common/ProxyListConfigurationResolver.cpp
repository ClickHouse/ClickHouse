#include <Common/ProxyListConfigurationResolver.h>

#include <Common/StringUtils/StringUtils.h>
#include <Poco/URI.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ProxyListConfigurationResolver::ProxyListConfigurationResolver(std::vector<Poco::URI> proxies_)
: proxies(std::move(proxies_))
{
}

std::optional<ProxyConfiguration> ProxyListConfigurationResolver::resolve(bool)
{
    /// Avoid atomic increment if number of proxies is 1.
    size_t index = proxies.size() > 1 ? (access_counter++) % proxies.size() : 0;

    auto & proxy = proxies[index];

    return ProxyConfiguration {proxy.getHost(), proxy.getScheme(), proxy.getPort()};
//    LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Use proxy: {}", proxies[index].toString());
}

}
