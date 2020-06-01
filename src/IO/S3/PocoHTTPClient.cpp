#include "PocoHTTPClient.h"

#include <utility>
#include <IO/HTTPCommon.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include "Poco/StreamCopier.h"
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <common/logger_useful.h>

namespace DB::S3
{
PocoHTTPClient::PocoHTTPClient(const Aws::Client::ClientConfiguration & clientConfiguration)
    : per_request_configuration(clientConfiguration.perRequestConfiguration)
    , timeouts(ConnectionTimeouts(
          Poco::Timespan(clientConfiguration.connectTimeoutMs * 1000), /// connection timeout.
          Poco::Timespan(clientConfiguration.httpRequestTimeoutMs * 1000), /// send timeout.
          Poco::Timespan(clientConfiguration.httpRequestTimeoutMs * 1000) /// receive timeout.
          ))
{
}

std::shared_ptr<Aws::Http::HttpResponse> PocoHTTPClient::MakeRequest(
    Aws::Http::HttpRequest & request,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    auto response = Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>("PocoHTTPClient", request);
    MakeRequestInternal(request, response, readLimiter, writeLimiter);
    return response;
}

std::shared_ptr<Aws::Http::HttpResponse> PocoHTTPClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest> & request,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    auto response = Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>("PocoHTTPClient", request);
    MakeRequestInternal(*request, response, readLimiter, writeLimiter);
    return response;
}

void PocoHTTPClient::MakeRequestInternal(
    Aws::Http::HttpRequest & request,
    std::shared_ptr<Aws::Http::Standard::StandardHttpResponse> & response,
    Aws::Utils::RateLimits::RateLimiterInterface *,
    Aws::Utils::RateLimits::RateLimiterInterface *) const
{
    Poco::Logger * log = &Poco::Logger::get("AWSClient");

    auto uri = request.GetUri().GetURIString();
    LOG_DEBUG(log, "Make request to: {}", uri);

    const int MAX_REDIRECT_ATTEMPTS = 10;
    try
    {
        for (int attempt = 0; attempt < MAX_REDIRECT_ATTEMPTS; ++attempt)
        {
            Poco::URI poco_uri(uri);

            /// Reverse proxy can replace host header with resolved ip address instead of host name.
            /// This can lead to request signature difference on S3 side.
            auto session = makeHTTPSession(poco_uri, timeouts, false);

            auto request_configuration = per_request_configuration(request);
            if (!request_configuration.proxyHost.empty())
                session->setProxy(request_configuration.proxyHost, request_configuration.proxyPort);

            Poco::Net::HTTPRequest poco_request(Poco::Net::HTTPRequest::HTTP_1_1);

            poco_request.setURI(poco_uri.getPathAndQuery());

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
            auto & request_body_stream = session->sendRequest(poco_request);

            if (request.GetContentBody())
            {
                LOG_DEBUG(log, "Writing request body.");
                if (attempt > 0) /// rewind content body buffer.
                {
                    request.GetContentBody()->clear();
                    request.GetContentBody()->seekg(0);
                }
                auto size = Poco::StreamCopier::copyStream(*request.GetContentBody(), request_body_stream);
                LOG_DEBUG(log, "Written {} bytes to request body", size);
            }

            LOG_DEBUG(log, "Receiving response...");
            auto & response_body_stream = session->receiveResponse(poco_response);

            int status_code = static_cast<int>(poco_response.getStatus());
            LOG_DEBUG(log, "Response status: {}, {}", status_code, poco_response.getReason());

            if (poco_response.getStatus() == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT)
            {
                auto location = poco_response.get("location");
                uri = location;
                LOG_DEBUG(log, "Redirecting request to new location: {}", location);

                continue;
            }

            response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(status_code));
            response->SetContentType(poco_response.getContentType());

            std::stringstream headers_ss;
            for (const auto & [header_name, header_value] : poco_response)
            {
                response->AddHeader(header_name, header_value);
                headers_ss << header_name << ": " << header_value << "; ";
            }
            LOG_DEBUG(log, "Received headers: {}", headers_ss.str());

            if (status_code >= 300)
            {
                String error_message;
                Poco::StreamCopier::copyToString(response_body_stream, error_message);

                response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
                response->SetClientErrorMessage(error_message);
            }
            else
                /// TODO: Do not copy whole stream.
                Poco::StreamCopier::copyStream(response_body_stream, response->GetResponseBody());

            break;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to make request to: {}", uri));
        response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
        response->SetClientErrorMessage(getCurrentExceptionMessage(false));
    }
}
}
