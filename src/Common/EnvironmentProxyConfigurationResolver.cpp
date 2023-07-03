#include "EnvironmentProxyConfigurationResolver.h"

#include <Poco/URI.h>

namespace DB
{

static constexpr auto PROXY_HTTP_ENVIRONMENT_VARIABLE = "http_proxy";
static constexpr auto PROXY_HTTPS_ENVIRONMENT_VARIABLE = "https_proxy";

std::optional<ProxyConfiguration> EnvironmentProxyConfigurationResolver::resolve(bool https)
{
    if (const auto * proxy_host = std::getenv(https ? PROXY_HTTPS_ENVIRONMENT_VARIABLE : PROXY_HTTP_ENVIRONMENT_VARIABLE))
    {
        auto uri = Poco::URI(proxy_host);
        return ProxyConfiguration {uri.getHost(), uri.getScheme(), uri.getPort()};
    }

    return {};
}

}
