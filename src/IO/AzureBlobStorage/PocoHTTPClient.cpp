#include <IO/AzureBlobStorage/PocoHTTPClient.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <IO/HTTPCommon.h>  // Add this include at the top
#include <Common/NetException.h>
#include <Common/Throttler.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/URI.h>
#include <azure/core/http/policies/policy.hpp>


namespace DB::ErrorCodes
{
    extern const int TOO_MANY_REDIRECTS;
    extern const int NOT_IMPLEMENTED;
}

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
}

namespace CurrentMetrics
{
    extern const Metric AzureRequests;
}

namespace HistogramMetrics
{
    extern MetricFamily & AzureBlobConnect;
    extern MetricFamily & DiskAzureConnect;
    extern MetricFamily & AzureFirstByte;
    extern MetricFamily & DiskAzureFirstByte;
}

namespace DB
{

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

    size_t OnRead(uint8_t *, size_t, Azure::Core::Context const &) override
    {
        return 0;
    }
};

class IStreamBodyStream : public Azure::Core::IO::BodyStream
{
private:
    std::istream & stream;
    std::istream::pos_type start_pos;
    DB::HTTPSessionPtr session;
    Int64 length;
public:
    IStreamBodyStream(std::istream & stream_, DB::HTTPSessionPtr session_, Int64 length_)
        : stream(stream_)
        , session(session_)
        , length(length_)
    {
        start_pos = stream.tellg();
    }

    int64_t Length() const override
    {
        return length;
    }

    void Rewind() override
    {
        stream.clear(); // Clear any error flags
        stream.seekg(start_pos);
    }

private:
    size_t OnRead(uint8_t * buffer, size_t count, Azure::Core::Context const &) override
    {
        if (!buffer || count == 0)
            return 0;

        stream.read(reinterpret_cast<char *>(buffer), static_cast<std::streamsize>(count));
        return static_cast<size_t>(stream.gcount());
    }
};

}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoAzureHTTPClient::Send(
    Azure::Core::Http::Request & request,
    Azure::Core::Context const & context)
{
    CurrentMetrics::Increment metric_increment{CurrentMetrics::AzureRequests};

    size_t redirects_left = max_redirects;
    // Just make a single request attempt, retries will be handled by the caller
    return makeRequestInternalImpl(request, context, redirects_left);
}

void PocoAzureHTTPClient::addMetric(const std::string & method, AzureMetricType type, ProfileEvents::Count amount) const
{
    static const ProfileEvents::Event events_map[static_cast<size_t>(AzureMetricType::EnumSize)][static_cast<size_t>(AzureMetricKind::EnumSize)] = {
        {ProfileEvents::AzureReadMicroseconds, ProfileEvents::AzureWriteMicroseconds},
        {ProfileEvents::AzureReadRequestsCount, ProfileEvents::AzureWriteRequestsCount},
        {ProfileEvents::AzureReadRequestsErrors, ProfileEvents::AzureWriteRequestsErrors},
        {ProfileEvents::AzureReadRequestsThrottling, ProfileEvents::AzureWriteRequestsThrottling},
        {ProfileEvents::AzureReadRequestsRedirects, ProfileEvents::AzureWriteRequestsRedirects},
    };

    static const ProfileEvents::Event disk_s3_events_map[static_cast<size_t>(AzureMetricType::EnumSize)][static_cast<size_t>(AzureMetricKind::EnumSize)] = {
        {ProfileEvents::DiskAzureReadMicroseconds, ProfileEvents::DiskAzureWriteMicroseconds},
        {ProfileEvents::DiskAzureReadRequestsCount, ProfileEvents::DiskAzureWriteRequestsCount},
        {ProfileEvents::DiskAzureReadRequestsErrors, ProfileEvents::DiskAzureWriteRequestsErrors},
        {ProfileEvents::DiskAzureReadRequestsThrottling, ProfileEvents::DiskAzureWriteRequestsThrottling},
        {ProfileEvents::DiskAzureReadRequestsRedirects, ProfileEvents::DiskAzureWriteRequestsRedirects},
    };

    auto kind = getMetricKind(method);

    ProfileEvents::increment(events_map[static_cast<unsigned int>(type)][static_cast<unsigned int>(kind)], amount);
    if (for_disk_azure)
        ProfileEvents::increment(disk_s3_events_map[static_cast<unsigned int>(type)][static_cast<unsigned int>(kind)], amount);
}

PocoAzureHTTPClient::AzureMetricKind PocoAzureHTTPClient::getMetricKind(const std::string & method) const
{
    if (method == "GET" || method == "HEAD")
        return AzureMetricKind::Read;
    else if (method == "POST" || method == "PUT" || method == "PATCH" || method == "DELETE")
        return AzureMetricKind::Write;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported request method: {}", method);
}

void PocoAzureHTTPClient::observeLatency(const std::string & method, AzureLatencyType type, HistogramMetrics::Value latency) const
{
    if (type == AzureLatencyType::Connect)
    {
        static HistogramMetrics::Metric & azure_connect_metric = HistogramMetrics::AzureBlobConnect.withLabels({});
        azure_connect_metric.observe(latency);

        if (for_disk_azure)
        {
            static HistogramMetrics::Metric & disk_azure_connect_metric = HistogramMetrics::DiskAzureConnect.withLabels({});
            disk_azure_connect_metric.observe(latency);
        }
        return;
    }

    const String attempt_label = [](const AzureLatencyType t)
    {
        switch (t)
        {
            case AzureLatencyType::FirstByteAttempt1: return "1";
            case AzureLatencyType::FirstByteAttempt2: return "2";
            case AzureLatencyType::FirstByteAttemptN: return "N";
            default: return "UNKNOWN";
        }
    }(type);

    const HistogramMetrics::LabelValues first_byte_label_values = {method, attempt_label};

    HistogramMetrics::observe(
        HistogramMetrics::AzureFirstByte, first_byte_label_values, latency);

    if (for_disk_azure)
    {
        HistogramMetrics::observe(
            HistogramMetrics::DiskAzureFirstByte, first_byte_label_values, latency);
    }
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
    , request_throttler(client_configuration.request_throttler)
    , extra_headers(client_configuration.extra_headers)
{}


ConnectionTimeouts PocoAzureHTTPClient::getTimeouts(const std::string & method, bool first_attempt, bool first_byte) const
{
    if (!use_adaptive_timeouts)
        return timeouts;

    return timeouts.getAdaptiveTimeouts(method, first_attempt, first_byte);
}

PocoAzureHTTPClient::AzureLatencyType PocoAzureHTTPClient::getByteLatencyType(size_t sdk_attempt, size_t ch_attempt) const
{
    AzureLatencyType result = AzureLatencyType::FirstByteAttempt1;
    if (sdk_attempt != 1 || ch_attempt != 1)
    {
        if ((sdk_attempt == 1 && ch_attempt == 2) || (sdk_attempt == 2 && ch_attempt == 1))
            result = AzureLatencyType::FirstByteAttempt2;
        else
            result = AzureLatencyType::FirstByteAttemptN;
    }
    return result;
}

const Azure::Core::Context::Key & PocoAzureHTTPClient::getSDKContextKeyForBufferRetry()
{
    static const Azure::Core::Context::Key buffer_retry_key;
    return buffer_retry_key;
}

static size_t getSDKRetryAttempt(const Azure::Core::Context & context)
{
    /// HACK to get the retry attempt number from the context.
    int retry_no = Azure::Core::Http::Policies::_internal::RetryPolicy::GetRetryCount(context);
    /// we will have only retry
    if (retry_no < 0)
        return 1; // First attempt is considered as attempt 1

    return retry_no + 1; // Convert to 1-based index
}

static size_t getCHRetryAttempt(const Azure::Core::Context & context)
{
    size_t number = 1;
    if (context.TryGetValue<size_t>(PocoAzureHTTPClient::getSDKContextKeyForBufferRetry(), number))
        return number + 1;

    return number;
}

std::unique_ptr<Azure::Core::Http::RawResponse> PocoAzureHTTPClient::makeRequestInternalImpl(
    Azure::Core::Http::Request & request,
    const Azure::Core::Context & context,
    size_t redirects_left)
{
    size_t sdk_retry_attempt = getSDKRetryAttempt(context);
    size_t ch_retry_attempt = getCHRetryAttempt(context);

    bool first_attempt = ch_retry_attempt == 1 && sdk_retry_attempt == 1;

    AzureLatencyType first_byte_latency_type = getByteLatencyType(ch_retry_attempt, sdk_retry_attempt);
    bool latency_recorded = false;
    UInt64 connect_time = 0;
    UInt64 first_byte_time = 0;

    const auto & headers = request.GetHeaders();
    const auto & method = request.GetMethod().ToString();
    const auto url = request.GetUrl();

    try
    {
        Poco::Net::HTTPRequest poco_request(Poco::Net::HTTPRequest::HTTP_1_1);

        poco_request.setMethod(method);

        std::string path_and_query = url.GetPath();

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

        poco_request.setURI("/" + path_and_query);

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

        if (method == "GET" || method == "HEAD")
            request_throttler.throttleHTTPGet();
        else if (method == "PUT" || method == "POST" || method == "PATCH")
            // Note that DELETE is free on Azure and thus we don't throttle it
            request_throttler.throttleHTTPPut();

        Poco::URI uri;
        uri.setScheme(url.GetScheme());
        uri.setHost(url.GetHost());
        uri.setPort(url.GetPort());
        uri.setPath("/");

        auto adaptive_timeouts = getTimeouts(method, first_attempt, true);

        auto group = for_disk_azure ? HTTPConnectionGroupType::DISK : HTTPConnectionGroupType::STORAGE;
        auto session = makeHTTPSession(
            group,
            uri,
            adaptive_timeouts,
            ProxyConfiguration{},
            &connect_time
        );

        Stopwatch watch;
        std::ostream & request_stream = session->sendRequest(poco_request, &connect_time, &first_byte_time);

        observeLatency(method, AzureLatencyType::Connect, connect_time);
        observeLatency(method, first_byte_latency_type, first_byte_time);
        latency_recorded = true;

        if (auto * body_stream = request.GetBodyStream(); body_stream != nullptr && body_stream->Length() > 0)
        {
            setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte*/ false));
            body_stream->Rewind();

            /// Manual copy
            std::vector<uint8_t> buffer(8192);
            while (auto read = body_stream->Read(buffer.data(), 8192))
            {
                if (read > 0)
                    request_stream.write(reinterpret_cast<const char *>(buffer.data()), read);
                else
                    break;
            }
        }

        // Set timeouts for receiving response
        setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte=*/false));

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
            response->SetHeader(header_name, header_value);

        // Track metrics
        addMetric(method, AzureMetricType::Microseconds, watch.elapsedMicroseconds());
        addMetric(method, AzureMetricType::Count);

        /// NOTE: Not even sure that there can be redirects in Azure, but let's handle it just in case.
        if (status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT)
        {
            addMetric(method, AzureMetricType::Redirects);

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
            addMetric(method, AzureMetricType::Throttling);
        }
        else if (status >= Poco::Net::HTTPResponse::HTTP_BAD_REQUEST)
        {
            addMetric(method, AzureMetricType::Errors);
        }

        return response;
    }
    catch (const Poco::TimeoutException &)
    {
        if (!latency_recorded)
        {
            observeLatency(method, AzureLatencyType::Connect, connect_time);
            observeLatency(method, first_byte_latency_type, first_byte_time);
        }

        auto error_message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true);
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, // HTTP/1.1
            Azure::Core::Http::HttpStatusCode::RequestTimeout,
            error_message.text);

        response->SetBodyStream(std::make_unique<EmptyBodyStream>());
        addMetric(method, AzureMetricType::Errors);
        return response;
    }
    catch (...)
    {
        if (!latency_recorded)
        {
            observeLatency(method, AzureLatencyType::Connect, connect_time);
            observeLatency(method, first_byte_latency_type, first_byte_time);
        }

        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, // HTTP/1.1
            Azure::Core::Http::HttpStatusCode::InternalServerError,
            getCurrentExceptionMessageAndPattern(true));

        response->SetBodyStream(std::make_unique<EmptyBodyStream>());

        addMetric(method, AzureMetricType::Errors);
        return response;
    }
}

}

#endif
