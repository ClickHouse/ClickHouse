#include "ProxyResolverConfiguration.h"

#if USE_AWS_S3

#include <utility>
#include <IO/HTTPCommon.h>
#include "Poco/StreamCopier.h"
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::S3
{
ProxyResolverConfiguration::ProxyResolverConfiguration(const Poco::URI & endpoint_, String proxy_scheme_, unsigned proxy_port_)
    : endpoint(endpoint_), proxy_scheme(std::move(proxy_scheme_)), proxy_port(proxy_port_)
{
}

Aws::Client::ClientConfigurationPerRequest ProxyResolverConfiguration::getConfiguration(const Aws::Http::HttpRequest &)
{
    LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Obtain proxy using resolver: {}", endpoint.toString());

    /// 1 second is enough for now.
    /// TODO: Make timeouts configurable.
    ConnectionTimeouts timeouts(
        Poco::Timespan(1000000), /// Connection timeout.
        Poco::Timespan(1000000), /// Send timeout.
        Poco::Timespan(1000000) /// Receive timeout.
    );
    auto session = makeHTTPSession(endpoint, timeouts);

    Aws::Client::ClientConfigurationPerRequest cfg;
    try
    {
        /// It should be just empty GET request.
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, endpoint.getPath(), Poco::Net::HTTPRequest::HTTP_1_1);
        session->sendRequest(request);

        Poco::Net::HTTPResponse response;
        auto & response_body_stream = session->receiveResponse(response);

        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception("Proxy resolver returned not OK status: " + response.getReason(), ErrorCodes::BAD_ARGUMENTS);

        String proxy_host;
        /// Read proxy host as string from response body.
        Poco::StreamCopier::copyToString(response_body_stream, proxy_host);

        LOG_DEBUG(&Poco::Logger::get("AWSClient"), "Use proxy: {}://{}:{}", proxy_scheme, proxy_host, proxy_port);

        cfg.proxyScheme = Aws::Http::SchemeMapper::FromString(proxy_scheme.c_str());
        cfg.proxyHost = proxy_host;
        cfg.proxyPort = proxy_port;

        return cfg;
    }
    catch (...)
    {
        tryLogCurrentException("AWSClient", "Failed to obtain proxy");
        /// Don't use proxy if it can't be obtained.
        return cfg;
    }
}

}

#endif
