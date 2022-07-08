#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Common/RemoteHostFilter.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/S3/SessionAwareIOStream.h>
#include <Storages/StorageS3Settings.h>

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>


namespace Aws::Http::Standard
{
class StandardHttpResponse;
}

namespace DB
{
class Context;
}

namespace DB::S3
{
class ClientFactory;

struct ClientConfigurationPerRequest
{
    Aws::Http::Scheme proxy_scheme = Aws::Http::Scheme::HTTPS;
    String proxy_host;
    unsigned proxy_port = 0;
};

struct PocoHTTPClientConfiguration : public Aws::Client::ClientConfiguration
{
    std::function<ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)> per_request_configuration = [] (const Aws::Http::HttpRequest &) { return ClientConfigurationPerRequest(); };
    String force_region;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects;
    bool enable_s3_requests_logging;
    HeaderCollection extra_headers;

    void updateSchemeAndRegion();

    std::function<void(const ClientConfigurationPerRequest &)> error_report;

private:
    PocoHTTPClientConfiguration(
        const String & force_region_,
        const RemoteHostFilter & remote_host_filter_,
        unsigned int s3_max_redirects_,
        bool enable_s3_requests_logging_
    );

    /// Constructor of Aws::Client::ClientConfiguration must be called after AWS SDK initialization.
    friend ClientFactory;
};

class PocoHTTPResponse : public Aws::Http::Standard::StandardHttpResponse
{
public:
    using SessionPtr = HTTPSessionPtr;

    explicit PocoHTTPResponse(const std::shared_ptr<const Aws::Http::HttpRequest> request)
        : Aws::Http::Standard::StandardHttpResponse(request)
        , body_stream(request->GetResponseStreamFactory())
    {
    }

    void SetResponseBody(Aws::IStream & incoming_stream, SessionPtr & session_) /// NOLINT
    {
        body_stream = Aws::Utils::Stream::ResponseStream(
            Aws::New<SessionAwareIOStream<SessionPtr>>("http result streambuf", session_, incoming_stream.rdbuf())
        );
    }

    Aws::IOStream & GetResponseBody() const override
    {
        return body_stream.GetUnderlyingStream();
    }

    Aws::Utils::Stream::ResponseStream && SwapResponseStreamOwnership() override
    {
        return std::move(body_stream);
    }

private:
    Aws::Utils::Stream::ResponseStream body_stream;
};

class PocoHTTPClient : public Aws::Http::HttpClient
{
public:
    explicit PocoHTTPClient(const PocoHTTPClientConfiguration & client_configuration);
    ~PocoHTTPClient() override = default;

    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest> & request,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const override;

private:
    void makeRequestInternal(
        Aws::Http::HttpRequest & request,
        std::shared_ptr<PocoHTTPResponse> & response,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const;

    std::function<ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)> per_request_configuration;
    std::function<void(const ClientConfigurationPerRequest &)> error_report;
    ConnectionTimeouts timeouts;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects;
    bool enable_s3_requests_logging;
    const HeaderCollection extra_headers;
};

}

#endif
