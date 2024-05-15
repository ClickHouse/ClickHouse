#include <Common/RemoteProxyConfigurationResolver.h>

#include <utility>
#include <IO/HTTPCommon.h>
#include <Poco/StreamCopier.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/logger_useful.h>
#include <Common/DNSResolver.h>
#include <IO/ReadWriteBufferFromHTTP.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::string RemoteProxyHostFetcherImpl::fetch(const Poco::URI & endpoint, const ConnectionTimeouts & timeouts) const
{
    /// It should be just empty GET request.
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, endpoint.getPath(), Poco::Net::HTTPRequest::HTTP_1_1);

    auto rw_http_buffer = BuilderRWBufferFromHTTP(endpoint)
                            .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                            .withTimeouts(timeouts)
                            .create({});

    String proxy_host;

    readString(proxy_host, *rw_http_buffer);

    return proxy_host;
}

RemoteProxyConfigurationResolver::RemoteProxyConfigurationResolver(
    const RemoteServerConfiguration & remote_server_configuration_,
    Protocol request_protocol_,
    std::unique_ptr<RemoteProxyHostFetcher> fetcher_,
    bool disable_tunneling_for_https_requests_over_http_proxy_
)
: ProxyConfigurationResolver(request_protocol_, disable_tunneling_for_https_requests_over_http_proxy_),
    remote_server_configuration(remote_server_configuration_), fetcher(std::move(fetcher_))
{
}

ProxyConfiguration RemoteProxyConfigurationResolver::resolve()
{
    auto logger = getLogger("RemoteProxyConfigurationResolver");

    auto & [endpoint, proxy_protocol_string, proxy_port, cache_ttl_] = remote_server_configuration;

    LOG_DEBUG(logger, "Obtain proxy using resolver: {}", endpoint.toString());

    std::lock_guard lock(cache_mutex);

    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();

    if (cache_ttl.count() && cache_valid && now <= cache_timestamp + cache_ttl && now >= cache_timestamp)
    {
        LOG_DEBUG(logger,
                  "Use cached proxy: {}://{}:{}. Tunneling: {}",
                  cached_config.protocol,
                  cached_config.host,
                  cached_config.port,
                  cached_config.tunneling);
        return cached_config;
    }

    /// 1 second is enough for now.
    /// TODO: Make timeouts configurable.
    auto timeouts = ConnectionTimeouts()
        .withConnectionTimeout(1)
        .withSendTimeout(1)
        .withReceiveTimeout(1);

    try
    {
        const auto proxy_host = fetcher->fetch(endpoint, timeouts);

        LOG_DEBUG(logger, "Use proxy: {}://{}:{}", proxy_protocol_string, proxy_host, proxy_port);

        auto proxy_protocol = ProxyConfiguration::protocolFromString(proxy_protocol_string);

        bool use_tunneling_for_https_requests_over_http_proxy = useTunneling(
            request_protocol,
            proxy_protocol,
            disable_tunneling_for_https_requests_over_http_proxy);

        cached_config.protocol = proxy_protocol;
        cached_config.host = proxy_host;
        cached_config.port = proxy_port;
        cached_config.tunneling = use_tunneling_for_https_requests_over_http_proxy;
        cached_config.original_request_protocol = request_protocol;
        cache_timestamp = std::chrono::system_clock::now();
        cache_valid = true;

        return cached_config;
    }
    catch (...)
    {
        tryLogCurrentException("RemoteProxyConfigurationResolver", "Failed to obtain proxy");
        return {};
    }
}

void RemoteProxyConfigurationResolver::errorReport(const ProxyConfiguration & config)
{
    if (config.host.empty())
        return;

    std::lock_guard lock(cache_mutex);

    if (!cache_ttl.count() || !cache_valid)
        return;

    if (std::tie(cached_config.protocol, cached_config.host, cached_config.port)
        != std::tie(config.protocol, config.host, config.port))
        return;

    /// Invalidate cached proxy when got error with this proxy
    cache_valid = false;
}

}
