#include <Common/RemoteProxyConfigurationResolver.h>

#include <utility>
#include <IO/HTTPCommon.h>
#include <Poco/StreamCopier.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

std::string RemoteProxyHostFetcherImpl::fetch(const Poco::URI & endpoint, const ConnectionTimeouts & timeouts)
{
    auto request = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_GET, endpoint.getPath(), Poco::Net::HTTPRequest::HTTP_1_1);
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, endpoint, timeouts);

    session->sendRequest(request);

    Poco::Net::HTTPResponse response;
    auto & response_body_stream = session->receiveResponse(response);

    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
        throw HTTPException(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            endpoint.toString(),
            response.getStatus(),
            response.getReason(),
            /* body_length = */ 0);

    std::string proxy_host;
    Poco::StreamCopier::copyToString(response_body_stream, proxy_host);

    return proxy_host;
}

RemoteProxyConfigurationResolver::RemoteProxyConfigurationResolver(
    const RemoteServerConfiguration & remote_server_configuration_,
    Protocol request_protocol_,
    const std::string & no_proxy_hosts_,
    std::shared_ptr<RemoteProxyHostFetcher> fetcher_,
    bool disable_tunneling_for_https_requests_over_http_proxy_
)
: ProxyConfigurationResolver(request_protocol_, disable_tunneling_for_https_requests_over_http_proxy_),
    remote_server_configuration(remote_server_configuration_), no_proxy_hosts(no_proxy_hosts_), fetcher(fetcher_)
{
}

ProxyConfiguration RemoteProxyConfigurationResolver::resolve()
{
    auto logger = getLogger("RemoteProxyConfigurationResolver");

    auto & [endpoint, proxy_protocol_string, proxy_port, cache_ttl] = remote_server_configuration;

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

    const auto proxy_host = fetcher->fetch(endpoint, timeouts);

    LOG_DEBUG(logger, "Use proxy: {}://{}:{}", proxy_protocol_string, proxy_host, proxy_port);

    auto proxy_protocol = ProxyConfiguration::protocolFromString(proxy_protocol_string);

    bool use_tunneling_for_https_requests_over_http_proxy = ProxyConfiguration::useTunneling(
        request_protocol,
        proxy_protocol,
        disable_tunneling_for_https_requests_over_http_proxy);

    cached_config.protocol = proxy_protocol;
    cached_config.host = proxy_host;
    cached_config.port = proxy_port;
    cached_config.tunneling = use_tunneling_for_https_requests_over_http_proxy;
    cached_config.original_request_protocol = request_protocol;
    cached_config.no_proxy_hosts = no_proxy_hosts;
    cache_timestamp = std::chrono::system_clock::now();
    cache_valid = true;

    return cached_config;
}

void RemoteProxyConfigurationResolver::errorReport(const ProxyConfiguration & config)
{
    if (config.host.empty())
        return;

    std::lock_guard lock(cache_mutex);

    if (!remote_server_configuration.cache_ttl_.count() || !cache_valid)
        return;

    if (std::tie(cached_config.protocol, cached_config.host, cached_config.port)
        != std::tie(config.protocol, config.host, config.port))
        return;

    /// Invalidate cached proxy when got error with this proxy
    cache_valid = false;
}

}
