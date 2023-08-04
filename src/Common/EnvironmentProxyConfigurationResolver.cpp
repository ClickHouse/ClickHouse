#include "EnvironmentProxyConfigurationResolver.h"

#include <Common/logger_useful.h>
#include <Poco/URI.h>

namespace DB
{

static constexpr auto PROXY_HTTP_ENVIRONMENT_VARIABLE = "http_proxy";
static constexpr auto PROXY_HTTPS_ENVIRONMENT_VARIABLE = "https_proxy";

EnvironmentProxyConfigurationResolver::EnvironmentProxyConfigurationResolver(Protocol protocol_)
    : protocol(protocol_)
{}

ProxyConfiguration EnvironmentProxyConfigurationResolver::resolve()
{
    bool https = protocol == Protocol::HTTPS;

    if (const auto * proxy_host = std::getenv(https ? PROXY_HTTPS_ENVIRONMENT_VARIABLE : PROXY_HTTP_ENVIRONMENT_VARIABLE)) // NOLINT(concurrency-mt-unsafe)
    {
        auto uri = Poco::URI(proxy_host);

        auto host = uri.getHost();
        auto scheme = uri.getScheme();
        auto port = uri.getPort();

        LOG_DEBUG(&Poco::Logger::get("EnvironmentProxyConfigurationResolver"), "Use proxy from environment: {}://{}:{}", scheme, host, port);

        return ProxyConfiguration {host, ProxyConfiguration::protocolFromString(scheme), port};
    }

    return {};
}

}
