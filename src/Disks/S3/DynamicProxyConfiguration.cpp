#include "DynamicProxyConfiguration.h"

#include <utility>

namespace DB::S3
{
DynamicProxyConfiguration::DynamicProxyConfiguration(std::vector<Poco::URI> _proxies) : proxies(std::move(_proxies)), access_counter(0)
{
}


Aws::Client::ClientConfigurationPerRequest DynamicProxyConfiguration::getConfiguration(const Aws::Http::HttpRequest &) const
{
    /// Avoid atomic increment if number of proxies is 1.
    size_t index = proxies.size() > 1 ? (access_counter++) % proxies.size() : 0;

    Aws::Client::ClientConfigurationPerRequest cfg;
    cfg.proxyHost = proxies[index].getHost();
    cfg.proxyPort = proxies[index].getPort();

    return cfg;
}

}
