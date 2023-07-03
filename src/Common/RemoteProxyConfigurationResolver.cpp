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

RemoteProxyConfigurationResolver::RemoteProxyConfigurationResolver(
    const Poco::URI & endpoint_,
    String proxy_scheme_,
    unsigned proxy_port_,
    unsigned cache_ttl_
)
: endpoint(endpoint_), proxy_scheme(std::move(proxy_scheme_)), proxy_port(proxy_port_), cache_ttl(cache_ttl_)
{
}

std::optional<ProxyConfiguration> RemoteProxyConfigurationResolver::resolve(bool)
{
    LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Obtain proxy using resolver: {}", endpoint.toString());

    std::lock_guard lock(cache_mutex);

    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();

    if (cache_ttl.count() && cache_valid && now <= cache_timestamp + cache_ttl && now >= cache_timestamp)
    {
//        LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Use cached proxy: {}://{}:{}", Aws::Http::SchemeMapper::ToString(cached_config.proxy_scheme), cached_config.proxy_host, cached_config.proxy_port);
        return cached_config;
    }

    /// 1 second is enough for now.
    /// TODO: Make timeouts configurable.
    ConnectionTimeouts timeouts(
        Poco::Timespan(1000000), /// Connection timeout.
        Poco::Timespan(1000000), /// Send timeout.
        Poco::Timespan(1000000)  /// Receive timeout.
    );

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
            session = makeHTTPSession(resolved_endpoint, timeouts, false);

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

        LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Use proxy: {}://{}:{}", proxy_scheme, proxy_host, proxy_port);

        cached_config.scheme = proxy_scheme;
        cached_config.host = proxy_host;
        cached_config.port = proxy_port;
        cache_timestamp = std::chrono::system_clock::now();
        cache_valid = true;

        return cached_config;
    }
    catch (...)
    {
        tryLogCurrentException("AWSClient", "Failed to obtain proxy");
        return std::nullopt;
    }
}

void RemoteProxyConfigurationResolver::errorReport(const ProxyConfiguration & config)
{    if (config.host.empty())
        return;

    std::lock_guard lock(cache_mutex);

    if (!cache_ttl.count() || !cache_valid)
        return;

    if (std::tie(cached_config.scheme, cached_config.host, cached_config.port)
        != std::tie(config.scheme, config.host, config.port))
        return;

    /// Invalidate cached proxy when got error with this proxy
    cache_valid = false;
}

}
