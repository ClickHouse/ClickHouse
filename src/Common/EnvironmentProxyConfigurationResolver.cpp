#include "EnvironmentProxyConfigurationResolver.h"

#include <Common/logger_useful.h>
#include <Poco/URI.h>

namespace DB
{

/*
 * Usually environment variables are upper-case, but it seems like proxy related variables are an exception.
 * See https://unix.stackexchange.com/questions/212894/whats-the-right-format-for-the-http-proxy-environment-variable-caps-or-no-ca/212972#212972
 * */
static constexpr auto PROXY_HTTP_ENVIRONMENT_VARIABLE = "http_proxy";
static constexpr auto PROXY_HTTPS_ENVIRONMENT_VARIABLE = "https_proxy";

EnvironmentProxyConfigurationResolver::EnvironmentProxyConfigurationResolver(
    Protocol request_protocol_, bool disable_tunneling_for_https_requests_over_http_proxy_)
    : ProxyConfigurationResolver(request_protocol_, disable_tunneling_for_https_requests_over_http_proxy_)
{}

namespace
{
    const char * getProxyHost(DB::ProxyConfiguration::Protocol protocol)
    {
        /*
         * getenv is safe to use here because ClickHouse code does not make any call to `setenv` or `putenv`
         * aside from tests and a very early call during startup: https://github.com/ClickHouse/ClickHouse/blob/master/src/Daemon/BaseDaemon.cpp#L791
         * */
        switch (protocol)
        {
            case ProxyConfiguration::Protocol::HTTP:
                return std::getenv(PROXY_HTTP_ENVIRONMENT_VARIABLE); // NOLINT(concurrency-mt-unsafe)
            case ProxyConfiguration::Protocol::HTTPS:
                return std::getenv(PROXY_HTTPS_ENVIRONMENT_VARIABLE); // NOLINT(concurrency-mt-unsafe)
        }
    }
}

ProxyConfiguration EnvironmentProxyConfigurationResolver::resolve()
{
    const auto * proxy_host = getProxyHost(request_protocol);

    if (!proxy_host)
    {
        return {};
    }

    auto uri = Poco::URI(proxy_host);
    auto host = uri.getHost();
    auto scheme = uri.getScheme();
    auto port = uri.getPort();

    LOG_TRACE(getLogger("EnvironmentProxyConfigurationResolver"), "Use proxy from environment: {}://{}:{}", scheme, host, port);

    return ProxyConfiguration {
        host,
        ProxyConfiguration::protocolFromString(scheme),
        port,
        useTunneling(request_protocol, ProxyConfiguration::protocolFromString(scheme), disable_tunneling_for_https_requests_over_http_proxy),
        request_protocol
    };
}

}
