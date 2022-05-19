#include "ProxyResolverConfiguration.h"

#if USE_AWS_S3

#include <utility>
#include <IO/HTTPCommon.h>
#include "Poco/StreamCopier.h"
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/logger_useful.h>
#include <Common/DNSResolver.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::S3
{

ProxyResolverConfiguration::ProxyResolverConfiguration(const Poco::URI & endpoint_, String proxy_scheme_
    , unsigned proxy_port_, unsigned cache_ttl_)
    : endpoint(endpoint_), proxy_scheme(std::move(proxy_scheme_)), proxy_port(proxy_port_), cache_ttl(cache_ttl_)
{
}

Aws::Client::ClientConfigurationPerRequest ProxyResolverConfiguration::getConfiguration(const Aws::Http::HttpRequest &)
{
    LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Obtain proxy using resolver: {}", endpoint.toString());

    std::unique_lock lock(cache_mutex);

    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();

    if (cache_ttl.count() && cache_valid && now <= cache_timestamp + cache_ttl && now >= cache_timestamp)
    {
        LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Use cached proxy: {}://{}:{}", Aws::Http::SchemeMapper::ToString(cached_config.proxyScheme), cached_config.proxyHost, cached_config.proxyPort);
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

        if (resolved_hosts.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Proxy resolver cannot resolve host {}", host);

        HTTPSessionPtr session;

        for (size_t i = 0; i < resolved_hosts.size(); ++i)
        {
            auto resolved_endpoint = endpoint;
            resolved_endpoint.setHost(resolved_hosts[i].toString());
            session = makeHTTPSession(resolved_endpoint, timeouts, false);

            try
            {
                session->sendRequest(request);
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
            throw Exception("Proxy resolver returned not OK status: " + response.getReason(), ErrorCodes::BAD_ARGUMENTS);

        String proxy_host;
        /// Read proxy host as string from response body.
        Poco::StreamCopier::copyToString(response_body_stream, proxy_host);

        LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Use proxy: {}://{}:{}", proxy_scheme, proxy_host, proxy_port);

        cached_config.proxyScheme = Aws::Http::SchemeMapper::FromString(proxy_scheme.c_str());
        cached_config.proxyHost = proxy_host;
        cached_config.proxyPort = proxy_port;
        cache_timestamp = std::chrono::system_clock::now();
        cache_valid = true;

        return cached_config;
    }
    catch (...)
    {
        tryLogCurrentException("AWSClient", "Failed to obtain proxy");
        /// Don't use proxy if it can't be obtained.
        Aws::Client::ClientConfigurationPerRequest cfg;
        return cfg;
    }
}

void ProxyResolverConfiguration::errorReport(const Aws::Client::ClientConfigurationPerRequest & config)
{
    if (config.proxyHost.empty())
        return;

    std::unique_lock lock(cache_mutex);

    if (!cache_ttl.count() || !cache_valid)
        return;

    if (cached_config.proxyScheme != config.proxyScheme || cached_config.proxyHost != config.proxyHost
            || cached_config.proxyPort != config.proxyPort)
        return;

    /// Invalidate cached proxy when got error with this proxy
    cache_valid = false;
}

}

#endif
