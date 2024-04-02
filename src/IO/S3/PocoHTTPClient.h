#pragma once

#include "config.h"

#include <string>
#include <vector>

#if USE_AWS_S3

#include <Common/RemoteHostFilter.h>
#include <Common/Throttler_fwd.h>
#include <Common/ProxyConfiguration.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/SessionAwareIOStream.h>

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

struct PocoHTTPClientConfiguration : public Aws::Client::ClientConfiguration
{
    std::function<DB::ProxyConfiguration()> per_request_configuration;
    String force_region;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects;
    bool enable_s3_requests_logging;
    bool for_disk_s3;
    ThrottlerPtr get_request_throttler;
    ThrottlerPtr put_request_throttler;
    HTTPHeaderEntries extra_headers;

    /// Not a client parameter in terms of HTTP and we won't send it to the server. Used internally to determine when connection have to be re-established.
    uint32_t http_keep_alive_timeout_ms = 0;
    /// Zero means pooling will not be used.
    size_t http_connection_pool_size = 0;
    /// See PoolBase::BehaviourOnLimit
    bool wait_on_pool_size_limit = true;

    void updateSchemeAndRegion();

    std::function<void(const DB::ProxyConfiguration &)> error_report;

private:
    PocoHTTPClientConfiguration(
        std::function<DB::ProxyConfiguration()> per_request_configuration_,
        const String & force_region_,
        const RemoteHostFilter & remote_host_filter_,
        unsigned int s3_max_redirects_,
        bool enable_s3_requests_logging_,
        bool for_disk_s3_,
        const ThrottlerPtr & get_request_throttler_,
        const ThrottlerPtr & put_request_throttler_,
        std::function<void(const DB::ProxyConfiguration &)> error_report_
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

    void SetResponseBody(Aws::IStream & incoming_stream, PooledHTTPSessionPtr & session_) /// NOLINT
    {
        body_stream = Aws::Utils::Stream::ResponseStream(
            Aws::New<SessionAwareIOStream<PooledHTTPSessionPtr>>("http result streambuf", session_, incoming_stream.rdbuf()));
    }

    void SetResponseBody(std::string & response_body) /// NOLINT
    {
        auto stream = Aws::New<std::stringstream>("http result buf", response_body); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        stream->exceptions(std::ios::failbit);
        body_stream = Aws::Utils::Stream::ResponseStream(std::move(stream));
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

    enum class S3MetricType
    {
        Microseconds,
        Count,
        Errors,
        Throttling,
        Redirects,

        EnumSize,
    };

    enum class S3MetricKind
    {
        Read,
        Write,

        EnumSize,
    };

    template <bool pooled>
    void makeRequestInternalImpl(
        Aws::Http::HttpRequest & request,
        const DB::ProxyConfiguration & per_request_configuration,
        std::shared_ptr<PocoHTTPResponse> & response,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const;

protected:
    static S3MetricKind getMetricKind(const Aws::Http::HttpRequest & request);
    void addMetric(const Aws::Http::HttpRequest & request, S3MetricType type, ProfileEvents::Count amount = 1) const;

    std::function<DB::ProxyConfiguration()> per_request_configuration;
    std::function<void(const DB::ProxyConfiguration &)> error_report;
    ConnectionTimeouts timeouts;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects;
    bool enable_s3_requests_logging;
    bool for_disk_s3;

    /// Limits get request per second rate for GET, SELECT and all other requests, excluding throttled by put throttler
    /// (i.e. throttles GetObject, HeadObject)
    ThrottlerPtr get_request_throttler;

    /// Limits put request per second rate for PUT, COPY, POST, LIST requests
    /// (i.e. throttles PutObject, CopyObject, ListObjects, CreateMultipartUpload, UploadPartCopy, UploadPart, CompleteMultipartUpload)
    /// NOTE: DELETE and CANCEL requests are not throttled by either put or get throttler
    ThrottlerPtr put_request_throttler;

    const HTTPHeaderEntries extra_headers;

    size_t http_connection_pool_size = 0;
    bool wait_on_pool_size_limit = true;
};

}

#endif
