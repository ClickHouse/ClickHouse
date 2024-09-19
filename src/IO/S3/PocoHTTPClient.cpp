#include <Poco/Timespan.h>
#include <Common/config_version.h>
#include "config.h"

#if USE_AWS_S3

#include "PocoHTTPClient.h"

#include <utility>
#include <algorithm>
#include <functional>

#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <Common/re2.h>
#include <IO/HTTPCommon.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/S3/ProviderType.h>
#include <Interpreters/Context.h>

#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include "Poco/StreamCopier.h"
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include <boost/algorithm/string.hpp>


static const int SUCCESS_RESPONSE_MIN = 200;
static const int SUCCESS_RESPONSE_MAX = 299;

namespace ProfileEvents
{
    extern const Event S3ReadMicroseconds;
    extern const Event S3ReadRequestsCount;
    extern const Event S3ReadRequestsErrors;
    extern const Event S3ReadRequestsThrottling;
    extern const Event S3ReadRequestsRedirects;

    extern const Event S3WriteMicroseconds;
    extern const Event S3WriteRequestsCount;
    extern const Event S3WriteRequestsErrors;
    extern const Event S3WriteRequestsThrottling;
    extern const Event S3WriteRequestsRedirects;

    extern const Event DiskS3ReadMicroseconds;
    extern const Event DiskS3ReadRequestsCount;
    extern const Event DiskS3ReadRequestsErrors;
    extern const Event DiskS3ReadRequestsThrottling;
    extern const Event DiskS3ReadRequestsRedirects;

    extern const Event DiskS3WriteMicroseconds;
    extern const Event DiskS3WriteRequestsCount;
    extern const Event DiskS3WriteRequestsErrors;
    extern const Event DiskS3WriteRequestsThrottling;
    extern const Event DiskS3WriteRequestsRedirects;

    extern const Event S3GetRequestThrottlerCount;
    extern const Event S3GetRequestThrottlerSleepMicroseconds;
    extern const Event S3PutRequestThrottlerCount;
    extern const Event S3PutRequestThrottlerSleepMicroseconds;

    extern const Event DiskS3GetRequestThrottlerCount;
    extern const Event DiskS3GetRequestThrottlerSleepMicroseconds;
    extern const Event DiskS3PutRequestThrottlerCount;
    extern const Event DiskS3PutRequestThrottlerSleepMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric S3Requests;
}

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_REDIRECTS;
}

namespace DB::S3
{

PocoHTTPClientConfiguration::PocoHTTPClientConfiguration(
        std::function<ProxyConfiguration()> per_request_configuration_,
        const String & force_region_,
        const RemoteHostFilter & remote_host_filter_,
        unsigned int s3_max_redirects_,
        unsigned int s3_retry_attempts_,
        bool enable_s3_requests_logging_,
        bool for_disk_s3_,
        bool s3_use_adaptive_timeouts_,
        const ThrottlerPtr & get_request_throttler_,
        const ThrottlerPtr & put_request_throttler_,
        std::function<void(const ProxyConfiguration &)> error_report_)
    : per_request_configuration(per_request_configuration_)
    , force_region(force_region_)
    , remote_host_filter(remote_host_filter_)
    , s3_max_redirects(s3_max_redirects_)
    , s3_retry_attempts(s3_retry_attempts_)
    , enable_s3_requests_logging(enable_s3_requests_logging_)
    , for_disk_s3(for_disk_s3_)
    , get_request_throttler(get_request_throttler_)
    , put_request_throttler(put_request_throttler_)
    , s3_use_adaptive_timeouts(s3_use_adaptive_timeouts_)
    , error_report(error_report_)
{
    /// This is used to identify configurations created by us.
    userAgent = std::string(VERSION_FULL) + VERSION_OFFICIAL;
}

void PocoHTTPClientConfiguration::updateSchemeAndRegion()
{
    if (!endpointOverride.empty())
    {
        static const RE2 region_pattern(R"(^s3[.\-]([a-z0-9\-]+)\.amazonaws\.)");
        Poco::URI uri(endpointOverride);
        if (uri.getScheme() == "http")
            scheme = Aws::Http::Scheme::HTTP;

        if (force_region.empty())
        {
            String matched_region;
            if (re2::RE2::PartialMatch(uri.getHost(), region_pattern, &matched_region))
            {
                boost::algorithm::to_lower(matched_region);
                region = matched_region;
            }
            else
            {
                /// In global mode AWS C++ SDK sends `us-east-1` but accepts switching to another one if being suggested.
                region = Aws::Region::AWS_GLOBAL;
            }
        }
        else
        {
            region = force_region;
        }
    }
}

ConnectionTimeouts getTimeoutsFromConfiguration(const PocoHTTPClientConfiguration & client_configuration)
{
    return ConnectionTimeouts()
        .withConnectionTimeout(Poco::Timespan(client_configuration.connectTimeoutMs * 1000))
        .withSendTimeout(Poco::Timespan(client_configuration.requestTimeoutMs * 1000))
        .withReceiveTimeout(Poco::Timespan(client_configuration.requestTimeoutMs * 1000))
        .withTCPKeepAliveTimeout(Poco::Timespan(
            client_configuration.enableTcpKeepAlive ? client_configuration.tcpKeepAliveIntervalMs * 1000 : 0))
        .withHTTPKeepAliveTimeout(Poco::Timespan(client_configuration.http_keep_alive_timeout, 0))
        .withHTTPKeepAliveMaxRequests(client_configuration.http_keep_alive_max_requests);
}

PocoHTTPClient::PocoHTTPClient(const PocoHTTPClientConfiguration & client_configuration)
    : per_request_configuration(client_configuration.per_request_configuration)
    , error_report(client_configuration.error_report)
    , timeouts(getTimeoutsFromConfiguration(client_configuration))
    , remote_host_filter(client_configuration.remote_host_filter)
    , s3_max_redirects(client_configuration.s3_max_redirects)
    , s3_use_adaptive_timeouts(client_configuration.s3_use_adaptive_timeouts)
    , enable_s3_requests_logging(client_configuration.enable_s3_requests_logging)
    , for_disk_s3(client_configuration.for_disk_s3)
    , get_request_throttler(client_configuration.get_request_throttler)
    , put_request_throttler(client_configuration.put_request_throttler)
    , extra_headers(client_configuration.extra_headers)
{
}

PocoHTTPClient::PocoHTTPClient(const Aws::Client::ClientConfiguration & client_configuration)
    : timeouts(ConnectionTimeouts()
       .withConnectionTimeout(Poco::Timespan(client_configuration.connectTimeoutMs * 1000))
       .withSendTimeout(Poco::Timespan(client_configuration.requestTimeoutMs * 1000))
       .withReceiveTimeout(Poco::Timespan(client_configuration.requestTimeoutMs * 1000))
       .withTCPKeepAliveTimeout(Poco::Timespan(
           client_configuration.enableTcpKeepAlive ? client_configuration.tcpKeepAliveIntervalMs * 1000 : 0))),
    remote_host_filter(Context::getGlobalContextInstance()->getRemoteHostFilter())
{
}

std::shared_ptr<Aws::Http::HttpResponse> PocoHTTPClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest> & request,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    try
    {
        auto response = Aws::MakeShared<PocoHTTPResponse>("PocoHTTPClient", request);
        makeRequestInternal(*request, response, readLimiter, writeLimiter);
        return response;
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(Exception::CreateFromPocoTag{}, e);
    }
    catch (const std::exception & e)
    {
        throw Exception(Exception::CreateFromSTDTag{}, e);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

namespace
{
    /// No comments:
    /// 1) https://aws.amazon.com/premiumsupport/knowledge-center/s3-resolve-200-internalerror/
    /// 2) https://github.com/aws/aws-sdk-cpp/issues/658
    bool checkRequestCanReturn2xxAndErrorInBody(Aws::Http::HttpRequest & request)
    {
        auto query_params = request.GetQueryStringParameters();
        if (request.HasHeader("x-amz-copy-source") || request.HasHeader("x-goog-copy-source"))
        {
            /// CopyObject https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
            if (query_params.empty())
                return true;

            /// UploadPartCopy https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
            if (query_params.contains("partNumber") && query_params.contains("uploadId"))
                return true;

        }
        else
        {
            /// CompleteMultipartUpload https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
            if (query_params.size() == 1 && query_params.contains("uploadId"))
                return true;
        }

        return false;
    }
}

PocoHTTPClient::S3MetricKind PocoHTTPClient::getMetricKind(const Aws::Http::HttpRequest & request)
{
    switch (request.GetMethod())
    {
        case Aws::Http::HttpMethod::HTTP_GET:
        case Aws::Http::HttpMethod::HTTP_HEAD:
            return S3MetricKind::Read;
        case Aws::Http::HttpMethod::HTTP_POST:
        case Aws::Http::HttpMethod::HTTP_DELETE:
        case Aws::Http::HttpMethod::HTTP_PUT:
        case Aws::Http::HttpMethod::HTTP_PATCH:
            return S3MetricKind::Write;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported request method");
}

void PocoHTTPClient::addMetric(const Aws::Http::HttpRequest & request, S3MetricType type, ProfileEvents::Count amount) const
{
    const ProfileEvents::Event events_map[static_cast<size_t>(S3MetricType::EnumSize)][static_cast<size_t>(S3MetricKind::EnumSize)] = {
        {ProfileEvents::S3ReadMicroseconds, ProfileEvents::S3WriteMicroseconds},
        {ProfileEvents::S3ReadRequestsCount, ProfileEvents::S3WriteRequestsCount},
        {ProfileEvents::S3ReadRequestsErrors, ProfileEvents::S3WriteRequestsErrors},
        {ProfileEvents::S3ReadRequestsThrottling, ProfileEvents::S3WriteRequestsThrottling},
        {ProfileEvents::S3ReadRequestsRedirects, ProfileEvents::S3WriteRequestsRedirects},
    };

    const ProfileEvents::Event disk_s3_events_map[static_cast<size_t>(S3MetricType::EnumSize)][static_cast<size_t>(S3MetricKind::EnumSize)] = {
        {ProfileEvents::DiskS3ReadMicroseconds, ProfileEvents::DiskS3WriteMicroseconds},
        {ProfileEvents::DiskS3ReadRequestsCount, ProfileEvents::DiskS3WriteRequestsCount},
        {ProfileEvents::DiskS3ReadRequestsErrors, ProfileEvents::DiskS3WriteRequestsErrors},
        {ProfileEvents::DiskS3ReadRequestsThrottling, ProfileEvents::DiskS3WriteRequestsThrottling},
        {ProfileEvents::DiskS3ReadRequestsRedirects, ProfileEvents::DiskS3WriteRequestsRedirects},
    };

    S3MetricKind kind = getMetricKind(request);

    ProfileEvents::increment(events_map[static_cast<unsigned int>(type)][static_cast<unsigned int>(kind)], amount);
    if (for_disk_s3)
        ProfileEvents::increment(disk_s3_events_map[static_cast<unsigned int>(type)][static_cast<unsigned int>(kind)], amount);
}

String extractAttemptFromInfo(const Aws::String & request_info)
{
    static auto key = Aws::String("attempt=");

    auto key_begin = request_info.find(key, 0);
    if (key_begin == Aws::String::npos)
        return "1";

    auto val_begin = key_begin + key.size();
    auto val_end = request_info.find(';', val_begin);
    if (val_end == Aws::String::npos)
        val_end = request_info.size();

    return request_info.substr(val_begin, val_end-val_begin);
}

String getOrEmpty(const Aws::Http::HeaderValueCollection & map, const String & key)
{
    auto it = map.find(key);
    if (it == map.end())
        return {};
    return it->second;
}

ConnectionTimeouts PocoHTTPClient::getTimeouts(const String & method, bool first_attempt, bool first_byte) const
{
    if (!s3_use_adaptive_timeouts)
        return timeouts;

    return timeouts.getAdaptiveTimeouts(method, first_attempt, first_byte);
}

void PocoHTTPClient::makeRequestInternal(
    Aws::Http::HttpRequest & request,
    std::shared_ptr<PocoHTTPResponse> & response,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    makeRequestInternalImpl(request, response, readLimiter, writeLimiter);
}

String getMethod(const Aws::Http::HttpRequest & request)
{
    switch (request.GetMethod())
    {
        case Aws::Http::HttpMethod::HTTP_GET:
            return Poco::Net::HTTPRequest::HTTP_GET;
        case Aws::Http::HttpMethod::HTTP_POST:
            return Poco::Net::HTTPRequest::HTTP_POST;
        case Aws::Http::HttpMethod::HTTP_DELETE:
            return Poco::Net::HTTPRequest::HTTP_DELETE;
        case Aws::Http::HttpMethod::HTTP_PUT:
            return Poco::Net::HTTPRequest::HTTP_PUT;
        case Aws::Http::HttpMethod::HTTP_HEAD:
            return Poco::Net::HTTPRequest::HTTP_HEAD;
        case Aws::Http::HttpMethod::HTTP_PATCH:
            return Poco::Net::HTTPRequest::HTTP_PATCH;
    }
}

void PocoHTTPClient::makeRequestInternalImpl(
    Aws::Http::HttpRequest & request,
    std::shared_ptr<PocoHTTPResponse> & response,
    Aws::Utils::RateLimits::RateLimiterInterface *,
    Aws::Utils::RateLimits::RateLimiterInterface *) const
{
    LoggerPtr log = getLogger("AWSClient");

    auto uri = request.GetUri().GetURIString();
    auto method = getMethod(request);

    auto sdk_attempt = extractAttemptFromInfo(getOrEmpty(request.GetHeaders(), Aws::Http::SDK_REQUEST_HEADER));
    auto ch_attempt = extractAttemptFromInfo(getOrEmpty(request.GetHeaders(), "clickhouse-request"));
    bool first_attempt = ch_attempt == "1" && sdk_attempt == "1";

    if (enable_s3_requests_logging)
        LOG_TEST(log, "Make request to: {}, aws sdk attempt: {}, clickhouse attempt: {}", uri, sdk_attempt, ch_attempt);

    switch (request.GetMethod())
    {
        case Aws::Http::HttpMethod::HTTP_GET:
        case Aws::Http::HttpMethod::HTTP_HEAD:
            if (get_request_throttler)
            {
                UInt64 sleep_us = get_request_throttler->add(1, ProfileEvents::S3GetRequestThrottlerCount, ProfileEvents::S3GetRequestThrottlerSleepMicroseconds);
                if (for_disk_s3)
                {
                    ProfileEvents::increment(ProfileEvents::DiskS3GetRequestThrottlerCount);
                    ProfileEvents::increment(ProfileEvents::DiskS3GetRequestThrottlerSleepMicroseconds, sleep_us);
                }
            }
            break;
        case Aws::Http::HttpMethod::HTTP_PUT:
        case Aws::Http::HttpMethod::HTTP_POST:
        case Aws::Http::HttpMethod::HTTP_PATCH:
            if (put_request_throttler)
            {
                UInt64 sleep_us = put_request_throttler->add(1, ProfileEvents::S3PutRequestThrottlerCount, ProfileEvents::S3PutRequestThrottlerSleepMicroseconds);
                if (for_disk_s3)
                {
                    ProfileEvents::increment(ProfileEvents::DiskS3PutRequestThrottlerCount);
                    ProfileEvents::increment(ProfileEvents::DiskS3PutRequestThrottlerSleepMicroseconds, sleep_us);
                }
            }
            break;
        case Aws::Http::HttpMethod::HTTP_DELETE:
            break; // Not throttled
    }

    addMetric(request, S3MetricType::Count);
    CurrentMetrics::Increment metric_increment{CurrentMetrics::S3Requests};

    try
    {
        ProxyConfiguration proxy_configuration;
        if (per_request_configuration)
            proxy_configuration = per_request_configuration();

        for (size_t attempt = 0; attempt <= s3_max_redirects; ++attempt)
        {
            Poco::URI target_uri(uri);

            if (enable_s3_requests_logging && !proxy_configuration.isEmpty())
                LOG_TEST(log, "Due to reverse proxy host name ({}) won't be resolved on ClickHouse side", uri);

            auto group = for_disk_s3 ? HTTPConnectionGroupType::DISK : HTTPConnectionGroupType::STORAGE;

            auto session = makeHTTPSession(
                group,
                target_uri,
                getTimeouts(method, first_attempt, /*first_byte*/ true),
                proxy_configuration);

            /// In case of error this address will be written to logs
            request.SetResolvedRemoteHost(session->getResolvedAddress());

            Poco::Net::HTTPRequest poco_request(Poco::Net::HTTPRequest::HTTP_1_1);

            /** According to RFC-2616, Request-URI is allowed to be encoded.
              * However, there is no clear agreement on which exact symbols must be encoded.
              * Effectively, `Poco::URI` chooses smaller subset of characters to encode,
              * whereas Amazon S3 and Google Cloud Storage expects another one.
              * In order to successfully execute a request, a path must be exact representation
              * of decoded path used by `AWSAuthSigner`.
              * Therefore we shall encode some symbols "manually" to fit the signatures.
              */

            std::string path_and_query;
            const std::string & query = target_uri.getRawQuery();
            const std::string reserved = "?#:;+@&=%"; /// Poco::URI::RESERVED_QUERY_PARAM without '/' plus percent sign.
            Poco::URI::encode(target_uri.getPath(), reserved, path_and_query);

            if (!query.empty())
            {
                path_and_query += '?';
                path_and_query += query;
            }

            /// `target_uri.getPath()` could return an empty string, but a proper HTTP request must
            /// always contain a non-empty URI in its first line (e.g. "POST / HTTP/1.1").
            if (path_and_query.empty())
                path_and_query = "/";

            poco_request.setURI(path_and_query);
            poco_request.setMethod(method);

            /// Headers coming from SDK are lower-cased.
            for (const auto & [header_name, header_value] : request.GetHeaders())
                poco_request.set(header_name, header_value);
            for (const auto & [header_name, header_value] : extra_headers)
            {
                // AWS S3 canonical headers must include `Host`, `Content-Type` and any `x-amz-*`.
                // These headers will be signed. Custom S3 headers specified in ClickHouse storage conf are added in `extra_headers`.
                // At this point in the stack trace, request has already been signed and any `x-amz-*` extra headers was already added
                // to the canonical headers list. Therefore, we should not add them again to the request.
                // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
                if (!header_name.starts_with("x-amz-"))
                {
                    poco_request.set(boost::algorithm::to_lower_copy(header_name), header_value);
                }
            }

            Poco::Net::HTTPResponse poco_response;

            Stopwatch watch;

            auto & request_body_stream = session->sendRequest(poco_request);

            if (request.GetContentBody())
            {
                if (enable_s3_requests_logging)
                    LOG_TEST(log, "Writing request body.");

                /// Rewind content body buffer.
                /// NOTE: we should do that always (even if `attempt == 0`) because the same request can be retried also by AWS,
                /// see retryStrategy in Aws::Client::ClientConfiguration.
                request.GetContentBody()->clear();
                request.GetContentBody()->seekg(0);

                setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte*/ false));
                auto size = Poco::StreamCopier::copyStream(*request.GetContentBody(), request_body_stream);
                if (enable_s3_requests_logging)
                    LOG_TEST(log, "Written {} bytes to request body", size);
            }

            setTimeouts(*session, getTimeouts(method, first_attempt, /*first_byte*/ false));

            if (enable_s3_requests_logging)
                LOG_TEST(log, "Receiving response...");
            auto & response_body_stream = session->receiveResponse(poco_response);

            watch.stop();
            addMetric(request, S3MetricType::Microseconds, watch.elapsedMicroseconds());

            int status_code = static_cast<int>(poco_response.getStatus());

            if (status_code >= SUCCESS_RESPONSE_MIN && status_code <= SUCCESS_RESPONSE_MAX)
            {
                if (enable_s3_requests_logging)
                    LOG_TEST(log, "Response status: {}, {}", status_code, poco_response.getReason());
            }
            else
            {
                /// Error statuses are more important so we show them even if `enable_s3_requests_logging == false`.
                LOG_INFO(log, "Response status: {}, {}", status_code, poco_response.getReason());
            }

            if (poco_response.getStatus() == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT)
            {
                auto location = poco_response.get("location");
                remote_host_filter.checkURL(Poco::URI(location));
                uri = location;
                if (enable_s3_requests_logging)
                    LOG_TEST(log, "Redirecting request to new location: {}", location);

                addMetric(request, S3MetricType::Redirects);
                continue;
            }

            response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(status_code));
            response->SetContentType(poco_response.getContentType());

            if (enable_s3_requests_logging)
            {
                WriteBufferFromOwnString headers_ss;
                for (const auto & [header_name, header_value] : poco_response)
                {
                    response->AddHeader(header_name, header_value);
                    headers_ss << header_name << ": " << header_value << "; ";
                }
                LOG_TEST(log, "Received headers: {}", headers_ss.str());
            }
            else
            {
                for (const auto & [header_name, header_value] : poco_response)
                    response->AddHeader(header_name, header_value);
            }

            /// Request is successful but for some special requests we can have actual error message in body
            if (status_code >= SUCCESS_RESPONSE_MIN && status_code <= SUCCESS_RESPONSE_MAX && checkRequestCanReturn2xxAndErrorInBody(request))
            {
                /// reading the full response
                std::string response_string((std::istreambuf_iterator<char>(response_body_stream)),
                               std::istreambuf_iterator<char>());

                /// Just trim string so it will not be so long
                LOG_TRACE(log, "Got dangerous response with successful code {}, checking its body: '{}'", status_code, response_string.substr(0, 300));
                const static std::string_view needle = "<Error>";
                if (auto it = std::search(response_string.begin(), response_string.end(), std::default_searcher(needle.begin(), needle.end())); it != response_string.end())
                {
                    LOG_WARNING(log, "Response for the request contains an <Error> tag in the body, will treat it as an internal server error (code 500)");
                    response->SetResponseCode(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR);

                    addMetric(request, S3MetricType::Errors);
                    if (error_report)
                        error_report(proxy_configuration);
                }

                /// Set response from string
                response->SetResponseBody(response_string);
            }
            else
            {
                if (status_code == 429 || status_code == 503)
                {
                    /// API throttling
                    addMetric(request, S3MetricType::Throttling);
                }
                else if (status_code >= 300)
                {
                    addMetric(request, S3MetricType::Errors);
                    if (status_code >= 500 && error_report)
                        error_report(proxy_configuration);
                }

                /// expose stream, after that client reads data from that stream without built-in retries
                response->SetResponseBody(response_body_stream, session);
            }

            return;
        }
        throw Exception(ErrorCodes::TOO_MANY_REDIRECTS, "Too many redirects while trying to access {}", request.GetUri().GetURIString());
    }
    catch (...)
    {
        auto error_message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true);
        error_message.text = fmt::format("Failed to make request to: {}: {}", uri, error_message.text);
        LOG_INFO(log, error_message);

        response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
        response->SetClientErrorMessage(getCurrentExceptionMessage(false));

        addMetric(request, S3MetricType::Errors);
    }
}

}

#endif
