#include <Common/EnvironmentProxyConfigurationResolver.h>

#include <Common/logger_useful.h>
#include <Common/proxyConfigurationToPocoProxyConfig.h>
#include <Poco/URI.h>

namespace DB
{

/*
 * Usually environment variables are upper-case, but it seems like proxy related variables are an exception.
 * See https://unix.stackexchange.com/questions/212894/whats-the-right-format-for-the-http-proxy-environment-variable-caps-or-no-ca/212972#212972
 * */
static constexpr auto PROXY_HTTP_ENVIRONMENT_VARIABLE = "http_proxy";
static constexpr auto PROXY_HTTPS_ENVIRONMENT_VARIABLE = "https_proxy";
static constexpr auto NO_PROXY_ENVIRONMENT_VARIABLE = "no_proxy";

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

    const char * getNoProxyHosts()
    {
        return std::getenv(NO_PROXY_ENVIRONMENT_VARIABLE); // NOLINT(concurrency-mt-unsafe)
    }

    ProxyConfiguration buildProxyConfiguration(
        ProxyConfiguration::Protocol request_protocol,
        const Poco::URI & uri,
        const std::string & no_proxy_hosts_string,
        bool disable_tunneling_for_https_requests_over_http_proxy)
    {
        if (uri.empty())
        {
            return {};
        }

        const auto & host = uri.getHost();
        const auto & scheme = uri.getScheme();
        const auto port = uri.getPort();

        const bool use_tunneling_for_https_requests_over_http_proxy = ProxyConfiguration::useTunneling(
            request_protocol,
            ProxyConfiguration::protocolFromString(scheme),
            disable_tunneling_for_https_requests_over_http_proxy);

        LOG_TRACE(getLogger("EnvironmentProxyConfigurationResolver"), "Use proxy from environment: {}://{}:{}", scheme, host, port);

        return ProxyConfiguration {
            host,
            ProxyConfiguration::protocolFromString(scheme),
            port,
            use_tunneling_for_https_requests_over_http_proxy,
            request_protocol,
            no_proxy_hosts_string
        };
    }
}

ProxyConfiguration EnvironmentProxyConfigurationResolver::resolve()
{
    static const auto * http_proxy_host = getProxyHost(Protocol::HTTP);
    static const auto * https_proxy_host = getProxyHost(Protocol::HTTPS);
    static const auto * no_proxy = getNoProxyHosts();
    static const auto poco_no_proxy_hosts = no_proxy ? buildPocoNonProxyHosts(no_proxy) : "";

    static const Poco::URI http_proxy_uri(http_proxy_host ? http_proxy_host : "");
    static const Poco::URI https_proxy_uri(https_proxy_host ? https_proxy_host : "");

    return buildProxyConfiguration(
        request_protocol,
        request_protocol == Protocol::HTTP ? http_proxy_uri : https_proxy_uri,
        poco_no_proxy_hosts,
        disable_tunneling_for_https_requests_over_http_proxy);
}

}
