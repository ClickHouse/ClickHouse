#include "ProxyListConfiguration.h"

#if USE_AWS_S3

#include <utility>
#include <Common/logger_useful.h>

namespace DB::S3
{
ProxyListConfiguration::ProxyListConfiguration(std::vector<Poco::URI> proxies_) : proxies(std::move(proxies_)), access_counter(0)
{
}


Aws::Client::ClientConfigurationPerRequest ProxyListConfiguration::getConfiguration(const Aws::Http::HttpRequest &)
{
    /// Avoid atomic increment if number of proxies is 1.
    size_t index = proxies.size() > 1 ? (access_counter++) % proxies.size() : 0;

    Aws::Client::ClientConfigurationPerRequest cfg;
    cfg.proxyScheme = Aws::Http::SchemeMapper::FromString(proxies[index].getScheme().c_str());
    cfg.proxyHost = proxies[index].getHost();
    cfg.proxyPort = proxies[index].getPort();

    LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Use proxy: {}", proxies[index].toString());

    return cfg;
}

}

#endif
