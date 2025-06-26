#include <IO/Azure/PocoHTTPClient.h>

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

std::unique_ptr<Azure::Core::Http::RawResponse> PocoAzureHTTPClient::Send(
    Azure::Core::Http::Request & request,
    Azure::Core::Context const & context)
{
    return makeRequestInternal(request, context);
}

void PocoAzureHTTPClient::addMetric(MetricType type, ProfileEvents::Count amount) const
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

PocoAzureHTTPClient::MetricKind PocoAzureHTTPClient::getMetricKind(const Azure::Core::Http::Request & request) const
{
    const auto & method = request.GetMethod().ToString();
    if (method == "GET" || method == "HEAD" || method == "POST")
        return MetricKind::Read;
    return MetricKind::Write;
}

void PocoAzureHTTPClient::addLatency(const Azure::Core::Http::Request & request, LatencyType type, LatencyBuckets::Count amount) const
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

ConnectionTimeouts PocoAzureHTTPClient::getTimeouts(const std::string & method, bool first_attempt, bool first_byte) const
{
    if (!use_adaptive_timeouts)
        return timeouts;

    return timeouts.getAdaptiveTimeouts(method, first_attempt, first_byte);
}

PocoAzureHTTPClient::LatencyType PocoAzureHTTPClient::getFirstByteLatencyType(const std::string & attempt) const
{
    if (attempt == "1")
        return LatencyType::FirstByteAttempt1;
    else if (attempt == "2")
        return LatencyType::FirstByteAttempt2;
    else
        return LatencyType::FirstByteAttemptN;
}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoAzureHTTPClient::makeRequestInternal(
    Azure::Core::Http::Request & request,
    const Azure::Core::Context & context)
{
    Stopwatch watch;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::AzureRequests};
    
    // Just make a single request attempt, retries will be handled by the caller
    return makeRequestInternalImpl(request, context);
}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoAzureHTTPClient::makeRequestInternalImpl(
    Azure::Core::Http::Request & request,
    const Azure::Core::Context & context)
{

    Stopwatch watch;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::AzureRequests};

    UInt64 connect_time = 0;
    UInt64 first_byte_time = 0;

    try
    {

        // Get request details
        const auto & headers = request.GetHeaders();
        const auto & method = request.GetMethod().ToString();
        const auto url = request.GetUrl();
        LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Making request to URL: {}", url.GetAbsoluteUrl());

        // Extract attempt information for latency tracking
        std::string attempt_str = "1";
        if (auto client_request_id = request.GetHeader("x-ms-client-request-id"); client_request_id.HasValue()) {
            // Parse attempt from client request ID or other headers
            // For Azure, we'll use a simple counter or default to "1" for first attempt
            attempt_str = "1"; // Could be enhanced to parse actual attempt numbers
        }
        
        bool first_attempt = (attempt_str == "1");
        LatencyType first_byte_latency_type = getFirstByteLatencyType(attempt_str);

        // Create POCO request
        Poco::Net::HTTPRequest poco_request(Poco::Net::HTTPRequest::HTTP_1_1);

        // Set method
        poco_request.setMethod(method);

        std::string path_and_query;
        // Set request URI with path
        const std::string reserved = "?#:;+@&=%"; /// Poco::URI::RESERVED_QUERY_PARAM without '/' plus percent sign.
        Poco::URI::encode(url.GetPath(), reserved, path_and_query);

        // Add query parameters if any
        auto query_params = url.GetQueryParameters();
        if (!query_params.empty())
        {
            path_and_query += "?";
            bool first = true;
            for (const auto& param : query_params)
            {
                if (!first)
                    path_and_query += "&";
                path_and_query += param.first + "=" + param.second;
                first = false;
            }
        }

        if (path_and_query.empty())
            path_and_query = "/";

        poco_request.setURI(path_and_query);


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
            {
                if (!header.name.starts_with("x-ms-"))
                    poco_request.set(Poco::toLower(header.name), header.value);
                else
                    poco_request.set(header.name, header.value);
            }
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
        Poco::URI uri;
        uri.setScheme(url.GetScheme());
        uri.setHost(url.GetHost());
        uri.setPort(url.GetPort());
        uri.setPath("/");

        auto http_method = request.GetMethod().ToString();
        auto adaptive_timeouts = getTimeouts(http_method, first_attempt, true);

        connect_time = first_byte_time = 0;

        std::ostringstream dump;
        poco_request.write(dump);
        LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Making request to {}", dump.str());
        LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Making request to URI: {}", uri.toString());

        auto group = HTTPConnectionGroupType::STORAGE;
        auto session = makeHTTPSession(
            group,
            uri,
            adaptive_timeouts,
            ProxyConfiguration{},
            &connect_time
        );

        // Send request
        std::ostream & request_stream = session->sendRequest(poco_request, &connect_time, &first_byte_time);

        addLatency(request, LatencyType::Connect, connect_time);
        addLatency(request, first_byte_latency_type, first_byte_time);

        // Handle request body
        if (auto * body_stream = request.GetBodyStream(); body_stream != nullptr && body_stream->Length() > 0)
        {
            LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Request has body stream {}", body_stream->Length());
            setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte*/ false));

            std::vector<uint8_t> buffer(8192); // 8KB buffer
            while (auto read = body_stream->Read(buffer.data(), 8192))
            {
                if (read > 0)
                    request_stream.write(reinterpret_cast<const char *>(buffer.data()), read);
                else
                    break; // End of stream
            }
        }

        // Set timeouts for receiving response
        setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte=*/true));

        // Get response
        Poco::Net::HTTPResponse poco_response;
        poco_response.setFieldLimit(static_cast<int>(http_max_fields));
        poco_response.setNameLengthLimit(static_cast<int>(http_max_field_name_size));
        poco_response.setValueLengthLimit(static_cast<int>(http_max_field_value_size));

        std::istream & response_stream = session->receiveResponse(poco_response);

        // Record first byte time
        first_byte_time = watch.elapsedMicroseconds();

            // Read the entire response into a buffer
        std::ostringstream oss;
        Poco::StreamCopier::copyStream(response_stream, oss);
        std::string response_body = oss.str();
        std::vector<uint8_t> response_body_bytes(response_body.begin(), response_body.end());

        LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Response body size: {} body {}", response_body_bytes.size(), response_body_bytes.empty() ? "empty" : "not empty");

        int status = static_cast<int>(poco_response.getStatus());

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, // HTTP/1.1
            static_cast<Azure::Core::Http::HttpStatusCode>(status),
            poco_response.getReason());

        for (const auto & [header_name, header_value] : poco_response)
            response->SetHeader(header_name, header_value);

        // Handle response
        // For streaming responses, we need to keep the session alive
        if (poco_response.getChunkedTransferEncoding() ||
            poco_response.getContentLength() == Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH)
        {
            response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(response_body_bytes));
        }
        else
        {
            response->SetBody(response_body_bytes);
        }

        // Track metrics
        auto latency = watch.elapsedMicroseconds();
        addMetric(MetricType::Microseconds, latency);
        addMetric(MetricType::Count);

        // Track first byte latency
        if (first_byte_time > 0)
        {
            addLatency(request, first_byte_latency_type, first_byte_time);
        }

        // Handle redirects

        LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Response status: {} {}", status, poco_response.getReason());
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

            throw Exception(ErrorCodes::TOO_MANY_REDIRECTS, "Too many redirects while trying to access {}", request.GetUrl().GetAbsoluteUrl());
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
                    error_report();
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
