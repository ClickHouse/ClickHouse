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

EnvironmentProxyConfigurationResolver::EnvironmentProxyConfigurationResolver(Protocol protocol_)
    : protocol(protocol_)
{}

namespace
{
    const char * getProxyHost(DB::ProxyConfiguration::Protocol protocol)
    {
        /*
         * getenv is safe to use here because ClickHouse code does not make any call to `setenv` or `putenv`
         * aside from tests and a very early call during startup: https://github.com/ClickHouse/ClickHouse/blob/master/src/Daemon/BaseDaemon.cpp#L791
         * */

        if (protocol == DB::ProxyConfiguration::Protocol::HTTP)
        {
            return std::getenv(PROXY_HTTP_ENVIRONMENT_VARIABLE); // NOLINT(concurrency-mt-unsafe)
        }
        else if (protocol == DB::ProxyConfiguration::Protocol::HTTPS)
        {
            return std::getenv(PROXY_HTTPS_ENVIRONMENT_VARIABLE); // NOLINT(concurrency-mt-unsafe)
        }
        else
        {
            if (const char * http_proxy_host = std::getenv(PROXY_HTTP_ENVIRONMENT_VARIABLE)) // NOLINT(concurrency-mt-unsafe)
            {
                return http_proxy_host;
            }
            else
            {
                return std::getenv(PROXY_HTTPS_ENVIRONMENT_VARIABLE); // NOLINT(concurrency-mt-unsafe)
            }
        }
    }
}

ProxyConfiguration EnvironmentProxyConfigurationResolver::resolve()
{
    const auto * proxy_host = getProxyHost(protocol);

    if (!proxy_host)
    {
        return {};
    }

    auto uri = Poco::URI(proxy_host);
    auto host = uri.getHost();
    auto scheme = uri.getScheme();
    auto port = uri.getPort();

    LOG_TRACE(&Poco::Logger::get("EnvironmentProxyConfigurationResolver"), "Use proxy from environment: {}://{}:{}", scheme, host, port);

    return ProxyConfiguration {
        host,
        ProxyConfiguration::protocolFromString(scheme),
        port
    };
}

}
