#pragma once

#include <Common/LatencyBuckets.h>
#include <Common/RemoteHostFilter.h>
#include <Common/Throttler_fwd.h>
#include <Common/ProxyConfiguration.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/SessionAwareIOStream.h>
#include <IO/HTTPHeaderEntries.h>

#include <azure/core/http/http.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/context.hpp>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

#include <functional>
#include <memory>

namespace DB
{

class Context;

struct PocoAzureHTTPClientConfiguration
{
    const RemoteHostFilter & remote_host_filter;
    UInt64 max_redirects;
    bool enable_requests_logging = false;
    bool for_disk_azure;

    ThrottlerPtr get_request_throttler;
    ThrottlerPtr put_request_throttler;
    HTTPHeaderEntries extra_headers;

    size_t connect_timeout_ms = 10000; // Default connection timeout in milliseconds
    size_t request_timeout_ms = 10000; // Default request timeout in milliseconds
    size_t tcp_keep_alive_interval_ms = 10000; // Default TCP keep-alive interval in milliseconds

    bool use_adaptive_timeouts = true;
    size_t http_keep_alive_timeout = DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT;
    size_t http_keep_alive_max_requests = DEFAULT_HTTP_KEEP_ALIVE_MAX_REQUEST;

    UInt64 http_max_fields = 1000000;
    UInt64 http_max_field_name_size = 128 * 1024;
    UInt64 http_max_field_value_size = 128 * 1024;
};


class PocoAzureHTTPClient : public Azure::Core::Http::HttpTransport
{
public:
    explicit PocoAzureHTTPClient(const PocoAzureHTTPClientConfiguration & client_configuration);

    ~PocoAzureHTTPClient() override = default;

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
        Azure::Core::Http::Request & request,
        Azure::Core::Context const & context) override;

private:
    enum class MetricType : uint8_t
    {
        Microseconds,
        Count,
        Errors,
        Throttling,
        Redirects,
        EnumSize,
    };

    enum class MetricKind : uint8_t
    {
        Read,
        Write,
        EnumSize,
    };

    enum class LatencyType : uint8_t
    {
        FirstByteAttempt1,
        FirstByteAttempt2,
        FirstByteAttemptN,
        Connect,
        EnumSize,
    };

    MetricKind getMetricKind(const Azure::Core::Http::Request & request) const;

    std::unique_ptr<Azure::Core::Http::RawResponse> makeRequestInternal(
        Azure::Core::Http::Request & request,
        const Azure::Core::Context & context);

    std::unique_ptr<Azure::Core::Http::RawResponse> makeRequestInternalImpl(
        Azure::Core::Http::Request & request,
        const Azure::Core::Context & context,
        size_t redirects_left);

    ConnectionTimeouts getTimeouts(const std::string & method, bool first_attempt, bool first_byte) const;
    LatencyType getByteLatencyType(size_t attempt_number) const;
    void addMetric(MetricType type, ProfileEvents::Count amount = 1) const;
    void addLatency(const Azure::Core::Http::Request & request, LatencyType type, LatencyBuckets::Count amount = 1) const;

    ConnectionTimeouts timeouts;
    const RemoteHostFilter & remote_host_filter;
    const unsigned int max_redirects = 0;
    bool use_adaptive_timeouts = true;
    const UInt64 http_max_fields;
    const UInt64 http_max_field_name_size;
    const UInt64 http_max_field_value_size;
    bool for_disk_azure = false;
    ThrottlerPtr get_request_throttler;
    ThrottlerPtr put_request_throttler;
    const HTTPHeaderEntries extra_headers;

};

} // namespace DB
