#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <Common/RemoteHostFilter.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/S3/SessionAwareIOStream.h>
#include <aws/core/client/ClientConfiguration.h> // Y_IGNORE
#include <aws/core/http/HttpClient.h> // Y_IGNORE
#include <aws/core/http/HttpRequest.h> // Y_IGNORE
#include <aws/core/http/standard/StandardHttpResponse.h> // Y_IGNORE

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

struct PocoHTTPClientConfiguration : public Aws::Client::ClientConfiguration
{
    String force_region;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects;

    void updateSchemeAndRegion();

private:
    PocoHTTPClientConfiguration(const String & force_region_, const RemoteHostFilter & remote_host_filter_, unsigned int s3_max_redirects_);

    /// Constructor of Aws::Client::ClientConfiguration must be called after AWS SDK initialization.
    friend ClientFactory;
};

class PocoHTTPResponse : public Aws::Http::Standard::StandardHttpResponse
{
public:
    using SessionPtr = HTTPSessionPtr;

    PocoHTTPResponse(const std::shared_ptr<const Aws::Http::HttpRequest> request)
        : Aws::Http::Standard::StandardHttpResponse(request)
        , body_stream(request->GetResponseStreamFactory())
    {
    }

    void SetResponseBody(Aws::IStream & incoming_stream, SessionPtr & session_)
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
    explicit PocoHTTPClient(const PocoHTTPClientConfiguration & clientConfiguration);
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

    std::function<Aws::Client::ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)> per_request_configuration;
    ConnectionTimeouts timeouts;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects;
};

}

#endif
