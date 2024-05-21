#include <Common/RemoteProxyConfigurationResolver.h>

#include <utility>
#include <IO/HTTPCommon.h>
#include <Poco/StreamCopier.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/logger_useful.h>
#include <Common/DNSResolver.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

RemoteProxyConfigurationResolver::RemoteProxyConfigurationResolver(
    const RemoteServerConfiguration & remote_server_configuration_,
    Protocol request_protocol_,
    bool disable_tunneling_for_https_requests_over_http_proxy_
)
: ProxyConfigurationResolver(request_protocol_, disable_tunneling_for_https_requests_over_http_proxy_), remote_server_configuration(remote_server_configuration_)
{
}

ProxyConfiguration RemoteProxyConfigurationResolver::resolve()
{
    auto logger = getLogger("RemoteProxyConfigurationResolver");

    auto & [endpoint, proxy_protocol, proxy_port, cache_ttl_] = remote_server_configuration;

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
        /// It should be just empty GET request.
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, endpoint.getPath(), Poco::Net::HTTPRequest::HTTP_1_1);

        const auto & host = endpoint.getHost();
        auto resolved_hosts = DNSResolver::instance().resolveHostAll(host);

        HTTPSessionPtr session;

        for (size_t i = 0; i < resolved_hosts.size(); ++i)
        {
            auto resolved_endpoint = endpoint;
            resolved_endpoint.setHost(resolved_hosts[i].toString());
            session = makeHTTPSession(HTTPConnectionGroupType::HTTP, resolved_endpoint, timeouts);

            try
            {
                session->sendRequest(request);
                break;
            }
            catch (...)
            {
                if (i + 1 == resolved_hosts.size())
                    throw;
            }
        }

        Poco::Net::HTTPResponse response;
        auto & response_body_stream = session->receiveResponse(response);

        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Proxy resolver returned not OK status: {}", response.getReason());

        String proxy_host;
        /// Read proxy host as string from response body.
        Poco::StreamCopier::copyToString(response_body_stream, proxy_host);

        LOG_DEBUG(logger, "Use proxy: {}://{}:{}", proxy_protocol, proxy_host, proxy_port);

        bool use_tunneling_for_https_requests_over_http_proxy = useTunneling(
            request_protocol,
            cached_config.protocol,
            disable_tunneling_for_https_requests_over_http_proxy);

        cached_config.protocol = ProxyConfiguration::protocolFromString(proxy_protocol);
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
