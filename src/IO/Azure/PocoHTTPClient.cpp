#include "PocoHTTPClient.h"

#include <IO/HTTPCommon.h>  // Add this include at the top
#include <Common/LatencyBuckets.h>
#include <Common/NetException.h>
#include <Common/Throttler.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <sstream>
#include <random>
#include <boost/algorithm/string/predicate.hpp>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_REDIRECTS;
    extern const int DNS_ERROR;
}

// Profile events definitions
namespace ProfileEvents
{
    extern const Event AzureReadMicroseconds;
    extern const Event AzureReadRequestsCount;
    extern const Event AzureReadRequestsErrors;
    extern const Event AzureReadRequestsThrottling;
    extern const Event AzureReadRequestsRedirects;

    extern const Event AzureWriteMicroseconds;
    extern const Event AzureWriteRequestsCount;
    extern const Event AzureWriteRequestsErrors;
    extern const Event AzureWriteRequestsThrottling;
    extern const Event AzureWriteRequestsRedirects;

    extern const Event DiskAzureReadMicroseconds;
    extern const Event DiskAzureReadRequestsCount;
    extern const Event DiskAzureReadRequestsErrors;
    extern const Event DiskAzureReadRequestsThrottling;
    extern const Event DiskAzureReadRequestsRedirects;

    extern const Event DiskAzureWriteMicroseconds;
    extern const Event DiskAzureWriteRequestsCount;
    extern const Event DiskAzureWriteRequestsErrors;
    extern const Event DiskAzureWriteRequestsThrottling;
    extern const Event DiskAzureWriteRequestsRedirects;

    extern const Event AzureGetRequestThrottlerCount;
    extern const Event AzureGetRequestThrottlerSleepMicroseconds;
    extern const Event AzurePutRequestThrottlerCount;
    extern const Event AzurePutRequestThrottlerSleepMicroseconds;

    extern const Event DiskAzureGetRequestThrottlerCount;
    extern const Event DiskAzureGetRequestThrottlerSleepMicroseconds;
    extern const Event DiskAzurePutRequestThrottlerCount;
    extern const Event DiskAzurePutRequestThrottlerSleepMicroseconds;
}

namespace LatencyBuckets
{
    extern const Event AzureFirstByteReadAttempt1Microseconds;
    extern const Event AzureFirstByteReadAttempt2Microseconds;
    extern const Event AzureFirstByteReadAttemptNMicroseconds;

    extern const Event AzureFirstByteWriteAttempt1Microseconds;
    extern const Event AzureFirstByteWriteAttempt2Microseconds;
    extern const Event AzureFirstByteWriteAttemptNMicroseconds;

    extern const Event DiskAzureFirstByteReadAttempt1Microseconds;
    extern const Event DiskAzureFirstByteReadAttempt2Microseconds;
    extern const Event DiskAzureFirstByteReadAttemptNMicroseconds;

    extern const Event DiskAzureFirstByteWriteAttempt1Microseconds;
    extern const Event DiskAzureFirstByteWriteAttempt2Microseconds;
    extern const Event DiskAzureFirstByteWriteAttemptNMicroseconds;

    extern const Event AzureConnectMicroseconds;
    extern const Event DiskAzureConnectMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric AzureRequests;
}

namespace DB
{

// PocoHTTPClient implementation
PocoHTTPClient::PocoHTTPClient(const PocoHTTPClientConfiguration & client_configuration)
    : per_request_configuration(client_configuration.per_request_configuration)
    , error_report(client_configuration.error_report)
    , timeouts()
    , remote_host_filter(client_configuration.remote_host_filter)
    , max_redirects(client_configuration.max_redirects)
    , use_adaptive_timeouts(client_configuration.use_adaptive_timeouts)
    , http_max_fields(client_configuration.http_max_fields)
    , http_max_field_name_size(client_configuration.http_max_field_name_size)
    , http_max_field_value_size(client_configuration.http_max_field_value_size)
    , enable_requests_logging(client_configuration.enable_requests_logging)
    , for_disk_azure(client_configuration.for_disk_azure)
    , get_request_throttler(client_configuration.get_request_throttler)
    , put_request_throttler(client_configuration.put_request_throttler)
    , extra_headers(client_configuration.extra_headers)
    , max_retry_attempts(client_configuration.max_retry_attempts)
    , initial_backoff_ms(client_configuration.initial_backoff_ms)
    , max_backoff_ms(client_configuration.max_backoff_ms)
{
}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoHTTPClient::Send(
    Azure::Core::Http::Request & request,
    Azure::Core::Context const & context)
{
    return makeRequestInternal(request, context);
}

void PocoHTTPClient::addMetric(MetricType type, ProfileEvents::Count amount) const
{
    if (for_disk_azure)
    {
        switch (type)
        {
            case MetricType::Microseconds:
                ProfileEvents::increment(ProfileEvents::DiskAzureReadMicroseconds, amount);
                break;
            case MetricType::Count:
                ProfileEvents::increment(ProfileEvents::DiskAzureReadRequestsCount, amount);
                break;
            case MetricType::Errors:
                ProfileEvents::increment(ProfileEvents::DiskAzureReadRequestsErrors, amount);
                break;
            case MetricType::Throttling:
                ProfileEvents::increment(ProfileEvents::DiskAzureReadRequestsThrottling, amount);
                break;
            case MetricType::Redirects:
                ProfileEvents::increment(ProfileEvents::DiskAzureReadRequestsRedirects, amount);
                break;
            default:
                break;
        }
    }
    else
    {
        switch (type)
        {
            case MetricType::Microseconds:
                ProfileEvents::increment(ProfileEvents::AzureReadMicroseconds, amount);
                break;
            case MetricType::Count:
                ProfileEvents::increment(ProfileEvents::AzureReadRequestsCount, amount);
                break;
            case MetricType::Errors:
                ProfileEvents::increment(ProfileEvents::AzureReadRequestsErrors, amount);
                break;
            case MetricType::Throttling:
                ProfileEvents::increment(ProfileEvents::AzureReadRequestsThrottling, amount);
                break;
            case MetricType::Redirects:
                ProfileEvents::increment(ProfileEvents::AzureReadRequestsRedirects, amount);
                break;
            default:
                break;
        }
    }
}

PocoHTTPClient::MetricKind PocoHTTPClient::getMetricKind(const Azure::Core::Http::Request & request) const
{
    const auto & method = request.GetMethod().ToString();
    if (method == "GET" || method == "HEAD" || method == "POST")
        return MetricKind::Read;
    return MetricKind::Write;
}

void PocoHTTPClient::addLatency(const Azure::Core::Http::Request & request, LatencyType type, LatencyBuckets::Count amount) const
{
    if (amount == 0)
        return;

    static const LatencyBuckets::Event events_map[static_cast<size_t>(LatencyType::EnumSize)][static_cast<size_t>(MetricKind::EnumSize)] = {
        {LatencyBuckets::AzureFirstByteReadAttempt1Microseconds, LatencyBuckets::AzureFirstByteWriteAttempt1Microseconds},
        {LatencyBuckets::AzureFirstByteReadAttempt2Microseconds, LatencyBuckets::AzureFirstByteWriteAttempt2Microseconds},
        {LatencyBuckets::AzureFirstByteReadAttemptNMicroseconds, LatencyBuckets::AzureFirstByteWriteAttemptNMicroseconds},
        {LatencyBuckets::AzureConnectMicroseconds, LatencyBuckets::AzureConnectMicroseconds},
    };

    static const LatencyBuckets::Event disk_azure_events_map[static_cast<size_t>(LatencyType::EnumSize)][static_cast<size_t>(MetricKind::EnumSize)] = {
        {LatencyBuckets::DiskAzureFirstByteReadAttempt1Microseconds, LatencyBuckets::DiskAzureFirstByteWriteAttempt1Microseconds},
        {LatencyBuckets::DiskAzureFirstByteReadAttempt2Microseconds, LatencyBuckets::DiskAzureFirstByteWriteAttempt2Microseconds},
        {LatencyBuckets::DiskAzureFirstByteReadAttemptNMicroseconds, LatencyBuckets::DiskAzureFirstByteWriteAttemptNMicroseconds},
        {LatencyBuckets::DiskAzureConnectMicroseconds, LatencyBuckets::DiskAzureConnectMicroseconds},
    };

    MetricKind kind = getMetricKind(request);

    LatencyBuckets::increment(events_map[static_cast<unsigned int>(type)][static_cast<unsigned int>(kind)], amount);
    if (for_disk_azure)
        LatencyBuckets::increment(disk_azure_events_map[static_cast<unsigned int>(type)][static_cast<unsigned int>(kind)], amount);
}

ConnectionTimeouts PocoHTTPClient::getTimeouts(const std::string & method, bool first_attempt, bool first_byte) const
{
    if (!use_adaptive_timeouts)
        return timeouts;

    return timeouts.getAdaptiveTimeouts(method, first_attempt, first_byte);
}

PocoHTTPClient::LatencyType PocoHTTPClient::getFirstByteLatencyType(const std::string & attempt) const
{
    if (attempt == "1")
        return LatencyType::FirstByteAttempt1;
    else if (attempt == "2")
        return LatencyType::FirstByteAttempt2;
    else
        return LatencyType::FirstByteAttemptN;
}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoHTTPClient::makeRequestInternal(
    Azure::Core::Http::Request & request,
    const Azure::Core::Context & context)
{
    Stopwatch watch;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::AzureRequests};
    
    // Just make a single request attempt, retries will be handled by the caller
    return makeRequestInternalImpl(request, context);
}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoHTTPClient::makeRequestInternalImpl(
    Azure::Core::Http::Request & request,
    const Azure::Core::Context & context)
{

    Stopwatch watch;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::AzureRequests};

    auto response = std::make_unique<Azure::Core::Http::RawResponse>(
        1, 1, // HTTP/1.1
        Azure::Core::Http::HttpStatusCode::Ok,
        "OK");

    UInt64 connect_time = 0;
    UInt64 first_byte_time = 0;

    try
    {
        // Get proxy configuration
        ProxyConfiguration proxy_configuration = this->proxy_configuration;
        if (per_request_configuration)
            proxy_configuration = per_request_configuration();

        // Get request details
        const auto & headers = request.GetHeaders();
        const auto & method = request.GetMethod().ToString();
        const auto url = request.GetUrl();

        // Create POCO request
        Poco::Net::HTTPRequest poco_request(
            "/",  // Will be overridden by setURI below
            Poco::Net::HTTPRequest::HTTP_1_1);

        // Set method
        poco_request.setMethod(method);

        // Set host header if not already set
        if (!headers.contains("Host"))
            poco_request.set("Host", url.GetHost());

        // Set request URI with path
        std::string path = url.GetPath();
        if (path.empty())
            path = "/";
            
        // Add query parameters if any
        auto query_params = url.GetQueryParameters();
        if (!query_params.empty()) {
            path += "?";
            bool first = true;
            for (const auto& param : query_params) {
                if (!first) path += "&";
                path += param.first + "=" + param.second;
                first = false;
            }
        }
        poco_request.setURI(path);

        // Determine if this is a retry attempt by checking x-ms-client-request-id header
        bool first_attempt = true;
        if (auto client_request_id = request.GetHeader("x-ms-client-request-id"); client_request_id.HasValue()) {
            // If the header exists, it's not the first attempt
            first_attempt = false;
        }

        // Set headers from the Azure request
        for (const auto & [name, value] : headers)
        {
            if (!value.empty())  // Skip empty headers
                poco_request.set(name, value);
        }

        // Set additional headers from configuration
        for (const auto & header : extra_headers)
        {
            if (!header.value.empty())  // Skip empty headers
                poco_request.set(header.name, header.value);
        }

        // Handle request body
        std::vector<uint8_t> body_buffer;
        size_t content_length = 0;

        if (auto * body_stream = request.GetBodyStream())
        {
            // Read the body stream into a buffer
            body_buffer = body_stream->ReadToEnd(context);
            content_length = body_buffer.size();
            poco_request.setContentLength(static_cast<std::streamsize>(content_length));
        }

        // Apply throttling
        if (method == "GET" || method == "HEAD" || method == "POST")
        {
            if (get_request_throttler)
            {
                UInt64 sleep_ns = get_request_throttler->add(1);
                UInt64 sleep_us = sleep_ns / 1000UL;
                ProfileEvents::increment(ProfileEvents::AzureGetRequestThrottlerCount);
                ProfileEvents::increment(ProfileEvents::AzureGetRequestThrottlerSleepMicroseconds, sleep_us);

                if (for_disk_azure)
                {
                    ProfileEvents::increment(ProfileEvents::DiskAzureGetRequestThrottlerCount);
                    ProfileEvents::increment(ProfileEvents::DiskAzureGetRequestThrottlerSleepMicroseconds, sleep_us);
                }
            }
        }
        else if (method == "PUT" || method == "POST" || method == "DELETE")
        {
            if (put_request_throttler)
            {
                UInt64 sleep_ns = put_request_throttler->add(1);
                UInt64 sleep_us = sleep_ns / 1000UL;
                ProfileEvents::increment(ProfileEvents::AzurePutRequestThrottlerCount);
                ProfileEvents::increment(ProfileEvents::AzurePutRequestThrottlerSleepMicroseconds, sleep_us);

                if (for_disk_azure)
                {
                    ProfileEvents::increment(ProfileEvents::DiskAzurePutRequestThrottlerCount);
                    ProfileEvents::increment(ProfileEvents::DiskAzurePutRequestThrottlerSleepMicroseconds, sleep_us);
                }
            }
        }

        // Create session using the common makeHTTPSession with adaptive timeouts
        Poco::URI uri(url.GetScheme() + "://" + url.GetHost() + (url.GetPort() ? ":" + std::to_string(url.GetPort()) : ""));

        auto http_method = request.GetMethod().ToString();
        auto adaptive_timeouts = getTimeouts(http_method, first_attempt, true);

        connect_time = first_byte_time = 0;

        auto session = makeHTTPSession(
            connection_group,
            uri,
            adaptive_timeouts,
            proxy_configuration,
            &connect_time
        );

        // Send request
        std::ostream & request_stream = session->sendRequest(poco_request);

        // Write request body if present
        if (!body_buffer.empty())
        {
            // Set timeouts for request body writing
            setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte=*/false));
            request_stream.write(reinterpret_cast<const char *>(body_buffer.data()), body_buffer.size());
        }

        // Set timeouts for receiving response
        setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte=*/true));

        // Get response
        Poco::Net::HTTPResponse poco_response;
        poco_response.setFieldLimit(static_cast<int>(http_max_fields));
        poco_response.setNameLengthLimit(static_cast<int>(http_max_field_name_size));
        poco_response.setValueLengthLimit(static_cast<int>(http_max_field_value_size));

        std::istream & response_stream = session->receiveResponse(poco_response);

        // Handle response
        // For streaming responses, we need to keep the session alive
        //if (poco_response.getChunkedTransferEncoding() ||
        //    poco_response.getContentLength() == Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH)
        //{
        //    response->SetBodyStream(response_stream_ptr);
        //}
        //else
        //{
            // Read the entire response into a buffer
            std::ostringstream oss;
            Poco::StreamCopier::copyStream(response_stream, oss);
            std::string response_body = oss.str();
            response->SetBody(std::vector<uint8_t>(response_body.begin(), response_body.end()));
        //}


        // Track metrics
        auto latency = watch.elapsedMicroseconds();
        addMetric(MetricType::Microseconds, latency);
        addMetric(MetricType::Count);

        // Handle redirects
        int status = static_cast<int>(poco_response.getStatus());
        if (status >= 300 && status < 400)
        {
            addMetric(MetricType::Redirects);

            if (max_redirects > 0)
            {
                std::string location = poco_response.get("Location", "");
                if (!location.empty())
                {
                    // Check if the redirect URL is allowed by the remote host filter
                    remote_host_filter.checkURL(Poco::URI(location));
                    
                    // Update request URL and retry
                    request.GetUrl() = Azure::Core::Url(location);
                    max_redirects--;
                    return makeRequestInternalImpl(request, context);
                }
            }

            throw Exception(ErrorCodes::TOO_MANY_REDIRECTS, "Too many redirects while trying to access {}", request.GetUri().GetURIString());
        }
        else if (status >= 300)
        {
            if (status == 429 || status == 503)
            {
                // API throttling
                addMetric(MetricType::Throttling);
            }
            else if (status >= 400)
            {
                addMetric(MetricType::Errors);
                // Optionally report errors for 5xx status codes
                if (status >= 500 && error_report)
                    error_report(proxy_configuration);
            }

            // Expose stream for error responses, allowing the client to read the error details
            // without built-in retries
            return response;
        }

        return response;
    }
    catch (const Poco::Exception & e)
    {
        addMetric(MetricType::Errors);
        LOG_ERROR(&Poco::Logger::get("AzureClient"), "Poco exception during request: {}", e.displayText());
        throw;
    }
    catch (const std::exception & e)
    {
        addMetric(MetricType::Errors);
        LOG_ERROR(&Poco::Logger::get("AzureClient"), "Exception during request: {}", e.what());
        throw;
    }
}

} // namespace DB::Azure
