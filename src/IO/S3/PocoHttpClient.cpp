#include "PocoHttpClient.h"

#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include <aws/core/monitoring/HttpClientMetrics.h>
#include <utility>
#include <IO/HTTPCommon.h>
#include "Poco/StreamCopier.h"
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <common/logger_useful.h>

namespace DB::S3
{
std::shared_ptr<Aws::Http::HttpResponse> PocoHttpClient::MakeRequest(
    Aws::Http::HttpRequest & request,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    auto response = Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>("PocoHttpClient", request);
    MakeRequestInternal(request, response, readLimiter, writeLimiter);
    return response;
}

std::shared_ptr<Aws::Http::HttpResponse> PocoHttpClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest> & request,
    Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const
{
    auto response = Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>("PocoHttpClient", request);
    MakeRequestInternal(*request, response, readLimiter, writeLimiter);
    return response;
}

void PocoHttpClient::MakeRequestInternal(
    Aws::Http::HttpRequest & request,
    std::shared_ptr<Aws::Http::Standard::StandardHttpResponse> & response,
    Aws::Utils::RateLimits::RateLimiterInterface *,
    Aws::Utils::RateLimits::RateLimiterInterface *) const
{
    auto uri = request.GetUri().GetURIString();

    LOG_DEBUG(&Logger::get("AWSClient"), "Make request to: {}", uri);

    const int MAX_REDIRECT_ATTEMPTS = 10;
    try
    {
        for (int attempt = 0; attempt < MAX_REDIRECT_ATTEMPTS; ++attempt)
        {
            /// 1 second is enough for now.
            /// TODO: Make timeouts configurable.
            ConnectionTimeouts timeouts(
                Poco::Timespan(2000000), /// Connection timeout.
                Poco::Timespan(2000000), /// Send timeout.
                Poco::Timespan(2000000) /// Receive timeout.
            );
            auto session = makeHTTPSession(Poco::URI(uri), timeouts);

            Poco::Net::HTTPRequest request_(Poco::Net::HTTPRequest::HTTP_1_1);

            request_.setURI(uri);

            switch (request.GetMethod())
            {
                case Aws::Http::HttpMethod::HTTP_GET:
                    request_.setMethod(Poco::Net::HTTPRequest::HTTP_GET);
                    break;
                case Aws::Http::HttpMethod::HTTP_POST:
                    request_.setMethod(Poco::Net::HTTPRequest::HTTP_POST);
                    break;
                case Aws::Http::HttpMethod::HTTP_DELETE:
                    request_.setMethod(Poco::Net::HTTPRequest::HTTP_DELETE);
                    break;
                case Aws::Http::HttpMethod::HTTP_PUT:
                    request_.setMethod(Poco::Net::HTTPRequest::HTTP_PUT);
                    break;
                case Aws::Http::HttpMethod::HTTP_HEAD:
                    request_.setMethod(Poco::Net::HTTPRequest::HTTP_HEAD);
                    break;
                case Aws::Http::HttpMethod::HTTP_PATCH:
                    request_.setMethod(Poco::Net::HTTPRequest::HTTP_PATCH);
                    break;
            }

            for (const auto & [header_name, header_value] : request.GetHeaders())
                request_.set(header_name, header_value);

            request_.setExpectContinue(true);

            Poco::Net::HTTPResponse response_;
            auto & request_body_stream = session->sendRequest(request_);

            if (session->peekResponse(response_))
            {
                if (request.GetContentBody())
                {
                    if (attempt > 0) /// rewind content body buffer.
                    {
                        request.GetContentBody()->clear();
                        request.GetContentBody()->seekg(0);
                    }
                    auto size = Poco::StreamCopier::copyStream(*request.GetContentBody(), request_body_stream);
                    LOG_DEBUG(
                        &Logger::get("AWSClient"), "Written {} bytes to request body", size);
                }
            }

            auto & response_body_stream = session->receiveResponse(response_);

            int status_code = static_cast<int>(response_.getStatus());
            LOG_DEBUG(
                &Logger::get("AWSClient"), "Response status: {}, {}", status_code, response_.getReason());

            if (response_.getStatus() == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT)
            {
                auto location = response_.get("location");
                uri = location;
                LOG_DEBUG(&Logger::get("AWSClient"), "Redirecting request to new location: {}", location);

                continue;
            }

            response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(status_code));
            response->SetContentType(response_.getContentType());

            std::stringstream headers_ss;
            for (const auto & [header_name, header_value] : response_)
            {
                response->AddHeader(header_name, header_value);
                headers_ss << " " << header_name << " : " << header_value << ";";
            }

            LOG_DEBUG(&Logger::get("AWSClient"), "Received headers:{}", headers_ss.str());

            /// TODO: Do not copy whole stream.
            Poco::StreamCopier::copyStream(response_body_stream, response->GetResponseBody());

            if (status_code >= 300)
            {
                response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
                response->SetClientErrorMessage(response_.getReason());
            }

            break;
        }
    }
    catch (...)
    {
        tryLogCurrentException(&Logger::get("AWSClient"), "Failed to make request to: " + uri);
        response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
        response->SetClientErrorMessage(getCurrentExceptionMessage(true));
    }
}
}
