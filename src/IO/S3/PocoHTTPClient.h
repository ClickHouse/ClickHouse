#pragma once

#include "config.h"

#include <string>
#include <vector>

#if USE_AWS_S3

#include <Common/HistogramMetrics.h>
#include <Common/RemoteHostFilter.h>
#include <Common/ProxyConfiguration.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/HTTPRequestThrottler.h>
#include <IO/SessionAwareIOStream.h>
#include <IO/S3Defines.h>

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>

#include <base/types.h>


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
class PocoHTTPClient;


struct PocoHTTPClientConfiguration : public Aws::Client::ClientConfiguration
{
    struct RetryStrategy
    {
        unsigned int max_retries = DEFAULT_RETRY_ATTEMPTS;
        unsigned int initial_delay_ms = DEFAULT_RETRY_INITIAL_DELAY_MS;
        unsigned int max_delay_ms = DEFAULT_RETRY_MAX_DELAY_MS;
        double jitter_factor = DEFAULT_RETRY_JITTER_FACTOR;
    };
    std::function<ProxyConfiguration()> per_request_configuration;
    String force_region;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects = DEFAULT_MAX_REDIRECTS;
    RetryStrategy retry_strategy;
    bool s3_slow_all_threads_after_network_error;
    bool s3_slow_all_threads_after_retryable_error;
    bool enable_s3_requests_logging;
    bool for_disk_s3;
    std::optional<std::string> opt_disk_name;
    HTTPRequestThrottler request_throttler;

    HTTPHeaderEntries extra_headers;
    String http_client;
    String service_account;
    String metadata_service;
    String request_token_path;

    /// See PoolBase::BehaviourOnLimit
    bool s3_use_adaptive_timeouts = true;
    size_t http_keep_alive_timeout = DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT;
    size_t http_keep_alive_max_requests = DEFAULT_HTTP_KEEP_ALIVE_MAX_REQUEST;

    UInt64 http_max_fields = 1000000;
    UInt64 http_max_field_name_size = 128 * 1024;
    UInt64 http_max_field_value_size = 128 * 1024;

    std::function<void(const ProxyConfiguration &)> error_report;

    void updateSchemeAndRegion();

private:
    PocoHTTPClientConfiguration(
        std::function<ProxyConfiguration()> per_request_configuration_,
        const String & force_region_,
        const RemoteHostFilter & remote_host_filter_,
        unsigned int s3_max_redirects_,
        RetryStrategy retry_strategy_,
        bool s3_slow_all_threads_after_network_error_,
        bool s3_slow_all_threads_after_retryable_error_,
        bool enable_s3_requests_logging_,
        bool for_disk_s3_,
        std::optional<std::string> opt_disk_name_,
        bool s3_use_adaptive_timeouts_,
        const HTTPRequestThrottler & request_throttler_,
        std::function<void(const ProxyConfiguration &)> error_report_);

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

    void SetResponseBody(std::string & response_body) /// NOLINT
    {
        auto * stream = Aws::New<std::stringstream>("http result buf", response_body); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
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
    explicit PocoHTTPClient(const Aws::Client::ClientConfiguration & client_configuration);
    ~PocoHTTPClient() override = default;

    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest> & request,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const override;

private:

    enum class S3MetricType : uint8_t
    {
        Microseconds,
        Count,
        Errors,
        Throttling,
        Redirects,

        EnumSize,
    };

    enum class S3LatencyType : uint8_t
    {
        FirstByteAttempt1,
        FirstByteAttempt2,
        FirstByteAttemptN,
        Connect,

        EnumSize,
    };

    enum class S3MetricKind : uint8_t
    {
        Read,
        Write,

        EnumSize,
    };

    ConnectionTimeouts getTimeouts(const String & method, bool first_attempt, bool first_byte) const;

    void makeRequestInternalImpl(
        Aws::Http::HttpRequest & request,
        std::shared_ptr<PocoHTTPResponse> & response,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const;

    static S3LatencyType getFirstByteLatencyType(const String & sdk_attempt, const String & ch_attempt);

protected:
    virtual void makeRequestInternal(
        Aws::Http::HttpRequest & request,
        std::shared_ptr<PocoHTTPResponse> & response,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const;

    static S3MetricKind getMetricKind(const Aws::Http::HttpRequest & request);
    void addMetric(const Aws::Http::HttpRequest & request, S3MetricType type, ProfileEvents::Count amount = 1) const;
    void observeLatency(const Aws::Http::HttpRequest & request, S3LatencyType type, HistogramMetrics::Value latency = 1) const;

    std::function<ProxyConfiguration()> per_request_configuration;
    std::function<void(const ProxyConfiguration &)> error_report;
    ConnectionTimeouts timeouts;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects = 0;
    bool s3_use_adaptive_timeouts = true;
    const UInt64 http_max_fields = 1000000;
    const UInt64 http_max_field_name_size = 128 * 1024;
    const UInt64 http_max_field_value_size = 128 * 1024;
    bool enable_s3_requests_logging = false;
    bool for_disk_s3 = false;

    HTTPRequestThrottler request_throttler;

    const HTTPHeaderEntries extra_headers;
};

class PocoHTTPClientGCPOAuth : public PocoHTTPClient
{
public:
    explicit PocoHTTPClientGCPOAuth(const PocoHTTPClientConfiguration & client_configuration);

    std::string getBearerToken() const;
private:
    void makeRequestInternal(
        Aws::Http::HttpRequest & request,
        std::shared_ptr<PocoHTTPResponse> & response,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const override;

    struct BearerToken
    {
        String token;
        std::chrono::system_clock::time_point is_valid_to;
    };

    const String service_account;
    const String metadata_service;
    const String request_token_path;

    mutable std::mutex mutex;
    mutable std::optional<BearerToken> bearer_token TSA_GUARDED_BY(mutex);

    BearerToken requestBearerToken() const TSA_REQUIRES(mutex);
};

}

#endif
