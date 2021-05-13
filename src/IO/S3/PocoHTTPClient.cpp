#include <Common/config.h>

#if USE_AWS_S3

#include "PocoHTTPClient.h"

#include <utility>
#include <IO/HTTPCommon.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/Stopwatch.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include "Poco/StreamCopier.h"
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <common/logger_useful.h>
#include <re2/re2.h>

#include <boost/algorithm/string.hpp>


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
}

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_REDIRECTS;
}

namespace DB::S3
{

PocoHTTPClientConfiguration::PocoHTTPClientConfiguration(
        const String & force_region_,
        const RemoteHostFilter & remote_host_filter_,
        unsigned int s3_max_redirects_)
    : force_region(force_region_)
    , remote_host_filter(remote_host_filter_)
    , s3_max_redirects(s3_max_redirects_)
{
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
                /// In global mode AWS C++ SDK send `us-east-1` but accept switching to another one if being suggested.
                region = Aws::Region::AWS_GLOBAL;
            }
        }
        else
        {
            region = force_region;
        }
    }
}


PocoHTTPClient::PocoHTTPClient(const PocoHTTPClientConfiguration & clientConfiguration)
    : per_request_configuration(clientConfiguration.perRequestConfiguration)
    , timeouts(ConnectionTimeouts(
          Poco::Timespan(clientConfiguration.connectTimeoutMs * 1000), /// connection timeout.
          Poco::Timespan(clientConfiguration.requestTimeoutMs * 1000), /// send timeout.
          Poco::Timespan(clientConfiguration.requestTimeoutMs * 1000) /// receive timeout.
          ))
    , remote_host_filter(clientConfiguration.remote_host_filter)
    , s3_max_redirects(clientConfiguration.s3_max_redirects)
{
}

std::shared_ptr<Aws::Http::HttpResponse> PocoHTTPClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest> & request,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    auto response = Aws::MakeShared<PocoHTTPResponse>("PocoHTTPClient", request);
    makeRequestInternal(*request, response, readLimiter, writeLimiter);
    return response;
}

void PocoHTTPClient::makeRequestInternal(
    Aws::Http::HttpRequest & request,
    std::shared_ptr<PocoHTTPResponse> & response,
    Aws::Utils::RateLimits::RateLimiterInterface *,
    Aws::Utils::RateLimits::RateLimiterInterface *) const
{
    Poco::Logger * log = &Poco::Logger::get("AWSClient");

    auto uri = request.GetUri().GetURIString();
    LOG_DEBUG(log, "Make request to: {}", uri);

    enum class S3MetricType
    {
        Microseconds,
        Count,
        Errors,
        Throttling,
        Redirects,

        EnumSize,
    };

    auto select_metric = [&request](S3MetricType type)
    {
        const ProfileEvents::Event events_map[][2] = {
            {ProfileEvents::S3ReadMicroseconds, ProfileEvents::S3WriteMicroseconds},
            {ProfileEvents::S3ReadRequestsCount, ProfileEvents::S3WriteRequestsCount},
            {ProfileEvents::S3ReadRequestsErrors, ProfileEvents::S3WriteRequestsErrors},
            {ProfileEvents::S3ReadRequestsThrottling, ProfileEvents::S3WriteRequestsThrottling},
            {ProfileEvents::S3ReadRequestsRedirects, ProfileEvents::S3WriteRequestsRedirects},
        };

        static_assert((sizeof(events_map) / sizeof(events_map[0])) == static_cast<unsigned int>(S3MetricType::EnumSize));

        switch (request.GetMethod())
        {
            case Aws::Http::HttpMethod::HTTP_GET:
            case Aws::Http::HttpMethod::HTTP_HEAD:
                return events_map[static_cast<unsigned int>(type)][0]; // Read
            case Aws::Http::HttpMethod::HTTP_POST:
            case Aws::Http::HttpMethod::HTTP_DELETE:
            case Aws::Http::HttpMethod::HTTP_PUT:
            case Aws::Http::HttpMethod::HTTP_PATCH:
                return events_map[static_cast<unsigned int>(type)][1]; // Write
        }

        throw Exception("Unsupported request method", ErrorCodes::NOT_IMPLEMENTED);
    };

    ProfileEvents::increment(select_metric(S3MetricType::Count));

    try
    {
        for (unsigned int attempt = 0; attempt <= s3_max_redirects; ++attempt)
        {
            Poco::URI target_uri(uri);

            /// Reverse proxy can replace host header with resolved ip address instead of host name.
            /// This can lead to request signature difference on S3 side.
            auto session = makeHTTPSession(target_uri, timeouts, false);

            auto request_configuration = per_request_configuration(request);

            if (!request_configuration.proxyHost.empty())
            {
                bool use_tunnel = request_configuration.proxyScheme == Aws::Http::Scheme::HTTP && target_uri.getScheme() == "https";

                session->setProxy(
                    request_configuration.proxyHost,
                    request_configuration.proxyPort,
                    Aws::Http::SchemeMapper::ToString(request_configuration.proxyScheme),
                    use_tunnel
                );
            }

            Poco::Net::HTTPRequest poco_request(Poco::Net::HTTPRequest::HTTP_1_1);

            /** Aws::Http::URI will encode URL in appropriate way for AWS S3 server.
              * Poco::URI also does that correctly but it's not compatible with AWS.
              * For example, `+` symbol will not be converted to `%2B` by Poco and would
              * be received as space symbol.
              *
              * References:
              * https://github.com/aws/aws-sdk-java/issues/1946
              * https://forums.aws.amazon.com/thread.jspa?threadID=55746
              *
              * Example:
              * Suppose we are requesting a file: abc+def.txt
              * To correctly do it, we need to construct an URL containing either:
              * - abc%2Bdef.txt
              * this is also technically correct:
              * - abc+def.txt
              * but AWS servers don't support it properly, interpreting plus character as whitespace
              * although it is in path part, not in query string.
              * e.g. this is not correct:
              * - abc%20def.txt
              *
              * Poco will keep plus character as is (which is correct) while AWS servers will treat it as whitespace, which is not what is intended.
              * To overcome this limitation, we encode URL with "Aws::Http::URI" and then pass already prepared URL to Poco.
              */

            Aws::Http::URI aws_target_uri(uri);
            poco_request.setURI(aws_target_uri.GetPath() + aws_target_uri.GetQueryString());

            switch (request.GetMethod())
            {
                case Aws::Http::HttpMethod::HTTP_GET:
                    poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_GET);
                    break;
                case Aws::Http::HttpMethod::HTTP_POST:
                    poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_POST);
                    break;
                case Aws::Http::HttpMethod::HTTP_DELETE:
                    poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_DELETE);
                    break;
                case Aws::Http::HttpMethod::HTTP_PUT:
                    poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_PUT);
                    break;
                case Aws::Http::HttpMethod::HTTP_HEAD:
                    poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_HEAD);
                    break;
                case Aws::Http::HttpMethod::HTTP_PATCH:
                    poco_request.setMethod(Poco::Net::HTTPRequest::HTTP_PATCH);
                    break;
            }

            for (const auto & [header_name, header_value] : request.GetHeaders())
                poco_request.set(header_name, header_value);

            Poco::Net::HTTPResponse poco_response;

            Stopwatch watch;

            auto & request_body_stream = session->sendRequest(poco_request);

            if (request.GetContentBody())
            {
                LOG_TRACE(log, "Writing request body.");

                if (attempt > 0) /// rewind content body buffer.
                {
                    request.GetContentBody()->clear();
                    request.GetContentBody()->seekg(0);
                }
                auto size = Poco::StreamCopier::copyStream(*request.GetContentBody(), request_body_stream);
                LOG_DEBUG(log, "Written {} bytes to request body", size);
            }

            LOG_TRACE(log, "Receiving response...");
            auto & response_body_stream = session->receiveResponse(poco_response);

            watch.stop();
            ProfileEvents::increment(select_metric(S3MetricType::Microseconds), watch.elapsedMicroseconds());

            int status_code = static_cast<int>(poco_response.getStatus());
            LOG_DEBUG(log, "Response status: {}, {}", status_code, poco_response.getReason());

            if (poco_response.getStatus() == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT)
            {
                auto location = poco_response.get("location");
                remote_host_filter.checkURL(Poco::URI(location));
                uri = location;
                LOG_DEBUG(log, "Redirecting request to new location: {}", location);

                ProfileEvents::increment(select_metric(S3MetricType::Redirects));

                continue;
            }

            response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(status_code));
            response->SetContentType(poco_response.getContentType());

            WriteBufferFromOwnString headers_ss;
            for (const auto & [header_name, header_value] : poco_response)
            {
                response->AddHeader(header_name, header_value);
                headers_ss << header_name << ": " << header_value << "; ";
            }
            LOG_DEBUG(log, "Received headers: {}", headers_ss.str());

            if (status_code == 429 || status_code == 503)
            { // API throttling
                ProfileEvents::increment(select_metric(S3MetricType::Throttling));
            }
            else if (status_code >= 300)
            {
                ProfileEvents::increment(select_metric(S3MetricType::Errors));
            }

            response->SetResponseBody(response_body_stream, session);

            return;
        }
        throw Exception(String("Too many redirects while trying to access ") + request.GetUri().GetURIString(),
            ErrorCodes::TOO_MANY_REDIRECTS);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to make request to: {}", uri));
        response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
        response->SetClientErrorMessage(getCurrentExceptionMessage(false));

        ProfileEvents::increment(select_metric(S3MetricType::Errors));
    }
}

}

#endif
