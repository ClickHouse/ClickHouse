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
#include <azure/core/http/policies/policy.hpp>

namespace
{

class EmptyBodyStream : public Azure::Core::IO::BodyStream
{
public:
    EmptyBodyStream() = default;

    int64_t Length() const override
    {
        return 0;
    }

    void Rewind() override
    {
    }

    size_t OnRead(uint8_t *, size_t, Azure::Core::Context const&) override
    {
        return 0;
    }
};

class IStreamBodyStream : public Azure::Core::IO::BodyStream
{
private:
    std::istream& m_stream;
    std::istream::pos_type m_start_pos;
    DB::HTTPSessionPtr session;
    int64_t m_length;


public:
    // If length is unknown, pass -1
    explicit IStreamBodyStream(std::istream& stream, DB::HTTPSessionPtr session_, int64_t length )
        : m_stream(stream), session(session_), m_length(length)
    {
        m_start_pos = m_stream.tellg();
    }

    int64_t Length() const override
    {
        //LOG_DEBUG(getLogger("PocoHTTPClient::IStreamBodyStream"), "LENGTH CALLED");
        return m_length;
    }

    void Rewind() override
    {
        //LOG_DEBUG(getLogger("PocoHTTPClient::IStreamBodyStream"), "REWIND CALLED");
        if (m_start_pos == std::istream::pos_type(-1))
        {
            throw std::runtime_error("IStreamBodyStream: Rewind not supported (non-seekable stream)");
        }
        m_stream.clear(); // Clear any error flags
        m_stream.seekg(m_start_pos);
        if (!m_stream)
        {
            throw std::runtime_error("IStreamBodyStream: Failed to rewind the stream");
        }
    }

private:
    size_t OnRead(uint8_t * buffer, size_t count, Azure::Core::Context const&) override
    {
        if (!buffer || count == 0)
        {
            return 0;
        }
        //LOG_DEBUG(getLogger("PocoHTTPClient::IStreamBodyStream"), "PocoHTTPClient::IStreamBodyStream::OnRead called with count = {}", count);
        m_stream.read(reinterpret_cast<char *>(buffer), static_cast<std::streamsize>(count));

        return static_cast<size_t>(m_stream.gcount());
    }
};

}

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


static ConnectionTimeouts getTimeoutsFromConfiguration(const PocoAzureHTTPClientConfiguration & client_configuration)
{
    return ConnectionTimeouts()
        .withConnectionTimeout(Poco::Timespan(client_configuration.connect_timeout_ms * 1000))
        .withSendTimeout(Poco::Timespan(client_configuration.request_timeout_ms * 1000))
        .withReceiveTimeout(Poco::Timespan(client_configuration.request_timeout_ms * 1000))
        .withTCPKeepAliveTimeout(Poco::Timespan(client_configuration.tcp_keep_alive_interval_ms * 1000))
        .withHTTPKeepAliveTimeout(Poco::Timespan(client_configuration.http_keep_alive_timeout, 0))
        .withHTTPKeepAliveMaxRequests(client_configuration.http_keep_alive_max_requests);
}

PocoAzureHTTPClient::PocoAzureHTTPClient(const PocoAzureHTTPClientConfiguration & client_configuration)
    : timeouts(getTimeoutsFromConfiguration(client_configuration))
    , remote_host_filter(client_configuration.remote_host_filter)
    , max_redirects(client_configuration.max_redirects)
    , use_adaptive_timeouts(client_configuration.use_adaptive_timeouts)
    , http_max_fields(client_configuration.http_max_fields)
    , http_max_field_name_size(client_configuration.http_max_field_name_size)
    , http_max_field_value_size(client_configuration.http_max_field_value_size)
    , for_disk_azure(client_configuration.for_disk_azure)
    , get_request_throttler(client_configuration.get_request_throttler)
    , put_request_throttler(client_configuration.put_request_throttler)
    , extra_headers(client_configuration.extra_headers)
{}


ConnectionTimeouts PocoAzureHTTPClient::getTimeouts(const std::string & method, bool first_attempt, bool first_byte) const
{
    if (!use_adaptive_timeouts)
        return timeouts;

    return timeouts.getAdaptiveTimeouts(method, first_attempt, first_byte);
}

PocoAzureHTTPClient::LatencyType PocoAzureHTTPClient::getByteLatencyType(size_t attempt_number) const
{
    if (attempt_number == 1)
        return LatencyType::FirstByteAttempt1;
    else if (attempt_number == 2)
        return LatencyType::FirstByteAttempt2;
    else
        return LatencyType::FirstByteAttemptN;
}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoAzureHTTPClient::makeRequestInternal(
    Azure::Core::Http::Request & request,
    const Azure::Core::Context & context)
{
    CurrentMetrics::Increment metric_increment{CurrentMetrics::AzureRequests};

    size_t redirects_left = max_redirects;
    // Just make a single request attempt, retries will be handled by the caller
    return makeRequestInternalImpl(request, context, redirects_left);
}


static size_t getRetryAttempt(const Azure::Core::Context & context)
{
    int retry_no = Azure::Core::Http::Policies::_internal::RetryPolicy::GetRetryCount(context);
    /// we will have only retry
    if (retry_no < 0)
        return 1; // First attempt is considered as attempt 1

    return retry_no + 1; // Convert to 1-based index
}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoAzureHTTPClient::makeRequestInternalImpl(
    Azure::Core::Http::Request & request,
    const Azure::Core::Context & context,
    size_t redirects_left)
{

    CurrentMetrics::Increment metric_increment{CurrentMetrics::AzureRequests};

    UInt64 connect_time = 0;
    UInt64 first_byte_time = 0;
    size_t retry_attempt = getRetryAttempt(context);

    LatencyType first_byte_latency_type = getByteLatencyType(retry_attempt);
    bool latency_recorded = false;
    try
    {
        // Get request details
        const auto & headers = request.GetHeaders();
        const auto & method = request.GetMethod().ToString();
        const auto url = request.GetUrl();
        //LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Making request to URL: {}", url.GetAbsoluteUrl());


        bool first_attempt = (retry_attempt == 1);

        // Create POCO request
        Poco::Net::HTTPRequest poco_request(Poco::Net::HTTPRequest::HTTP_1_1);

        // Set method
        poco_request.setMethod(method);

        std::string path_and_query = url.GetPath();
        // Add query parameters if any
        auto query_params = url.GetQueryParameters();
        if (!query_params.empty())
        {
            path_and_query += "?";
            bool first = true;
            for (const auto & param : query_params)
            {
                if (!first)
                    path_and_query += "&";
                path_and_query += param.first + "=" + param.second;
                first = false;
            }
        }

        if (path_and_query.empty())
            path_and_query = "/";

        //LOG_DEBUG(getLogger("DEBUG"), "PATH AND QUERY {}", path_and_query);
        poco_request.setURI("/" + path_and_query);

        // Set headers from the Azure request
        for (const auto & [name, value] : headers)
        {
            //LOG_DEBUG(getLogger("DEBUG"), "HEADER {}: {}", name, value);
            if (!value.empty())  // Skip empty headers
                poco_request.set(name, value);
        }
        // Set additional headers from configuration
        for (const auto & header : extra_headers)
        {
            if (!header.value.empty())  // Skip empty headers
            {
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

        //std::ostringstream dump;
        //poco_request.write(dump);
        //LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Making request to {}", dump.str());
        //LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Making request to URI: {}", uri.toString());

        auto group = for_disk_azure ? HTTPConnectionGroupType::DISK : HTTPConnectionGroupType::STORAGE;
        auto session = makeHTTPSession(
            group,
            uri,
            adaptive_timeouts,
            ProxyConfiguration{},
            &connect_time
        );

        // Send request
        Stopwatch watch;
        std::ostream & request_stream = session->sendRequest(poco_request, &connect_time, &first_byte_time);

        addLatency(request, LatencyType::Connect, connect_time);
        addLatency(request, first_byte_latency_type, first_byte_time);
        latency_recorded = true;

        // Handle request body
        if (auto * body_stream = request.GetBodyStream(); body_stream != nullptr && body_stream->Length() > 0)
        {
            //LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Request has body stream {}", body_stream->Length());
            setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte*/ false));
            body_stream->Rewind();

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

        int status = static_cast<int>(poco_response.getStatus());

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, // HTTP/1.1
            static_cast<Azure::Core::Http::HttpStatusCode>(status),
            poco_response.getReason());

        response->SetBodyStream(std::make_unique<IStreamBodyStream>(response_stream, session, poco_response.getContentLength()));

        for (const auto & [header_name, header_value] : poco_response)
        {
            //LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Response headers: {} => {}", header_name, header_value);
            response->SetHeader(header_name, header_value);
        }

        // Track metrics
        addMetric(MetricType::Microseconds, watch.elapsedMicroseconds());
        addMetric(MetricType::Count);

        //LOG_DEBUG(&Poco::Logger::get("AzureClient"), "Response status: {} {}", status, poco_response.getReason());

        if (status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT)
        {
            addMetric(MetricType::Redirects);

            if (redirects_left > 0)
            {
                auto location = poco_response.get("location");
                remote_host_filter.checkURL(Poco::URI(location));

                if (!location.empty())
                {
                    // Check if the redirect URL is allowed by the remote host filter
                    remote_host_filter.checkURL(Poco::URI(location));
                    // Update request URL and retry
                    request.GetUrl() = Azure::Core::Url(location);
                    return makeRequestInternalImpl(request, context, redirects_left - 1);
                }
            }

            throw Exception(ErrorCodes::TOO_MANY_REDIRECTS, "Too many redirects while trying to access {}", request.GetUrl().GetAbsoluteUrl());
        }

        if (status == Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS || status == Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE)
        {
            // API throttling
            addMetric(MetricType::Throttling);
        }
        else if (status >= Poco::Net::HTTPResponse::HTTP_BAD_REQUEST)
        {
            addMetric(MetricType::Errors);
        }

        //LOG_DEBUG(&Poco::Logger::get("AzureClient"), "RETURN RESPONSE");
        return response;
    }
    catch (const Poco::TimeoutException &)
    {
        if (!latency_recorded)
        {
            addLatency(request, LatencyType::Connect, connect_time);
            addLatency(request, first_byte_latency_type, first_byte_time);
        }

        auto error_message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true);
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, // HTTP/1.1
            Azure::Core::Http::HttpStatusCode::RequestTimeout,
            error_message.text);

        response->SetBodyStream(std::make_unique<EmptyBodyStream>());
        //LOG_INFO(&Poco::Logger::get("AzureClient"), error_message);
        addMetric(MetricType::Errors);
        return response;
    }
    catch (...)
    {
        if (!latency_recorded)
        {
            addLatency(request, LatencyType::Connect, connect_time);
            addLatency(request, first_byte_latency_type, first_byte_time);
        }

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, // HTTP/1.1
            Azure::Core::Http::HttpStatusCode::InternalServerError,
            getCurrentExceptionMessageAndPattern(true));

        response->SetBodyStream(std::make_unique<EmptyBodyStream>());

        addMetric(MetricType::Errors);
        return response;
    }
}

} // namespace DB::Azure
