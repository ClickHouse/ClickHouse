#include "DynamicProxyConfiguration.h"

#include <utility>
#include <common/logger_useful.h>

namespace DB::S3
{
DynamicProxyConfiguration::DynamicProxyConfiguration(std::vector<Poco::URI> _proxies) : proxies(std::move(_proxies)), access_counter(0)
{
}


Aws::Client::ClientConfigurationPerRequest DynamicProxyConfiguration::getConfiguration(const Aws::Http::HttpRequest &)
{
    /// Avoid atomic increment if number of proxies is 1.
    size_t index = proxies.size() > 1 ? (access_counter++) % proxies.size() : 0;

    Aws::Client::ClientConfigurationPerRequest cfg;
    cfg.proxyHost = proxies[index].getHost();
    cfg.proxyPort = proxies[index].getPort();

    LOG_DEBUG(&Logger::get("AWSClient"), "Use proxy: " << proxies[index].toString());

    return cfg;
}

}
