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

    /// 1 second is enough for now.
    /// TODO: Make timeouts configurable.
    ConnectionTimeouts timeouts(
        Poco::Timespan(1000000), /// Connection timeout.
        Poco::Timespan(1000000), /// Send timeout.
        Poco::Timespan(1000000) /// Receive timeout.
    );
    auto session = makeHTTPSession(Poco::URI(uri), timeouts);

    LOG_DEBUG(&Logger::get("AWSClient"), "Here 1");

    try
    {
        Poco::Net::HTTPRequest request_(Poco::Net::HTTPRequest::HTTP_1_1);

        request_.setURI(uri);

        LOG_DEBUG(&Logger::get("AWSClient"), "Here 2");

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

        LOG_DEBUG(&Logger::get("AWSClient"), "Here 3");

        for (const auto & [header_name, header_value] : request.GetHeaders())
            request_.set(header_name, header_value);

        LOG_DEBUG(&Logger::get("AWSClient"), "Here 4");

        auto & request_body_stream = session->sendRequest(request_);

        if (request.GetContentBody())
            Poco::StreamCopier::copyStream(*(request.GetContentBody()), request_body_stream);
        else
            LOG_ERROR(&Logger::get("AWSClient"), "No content body :(");

        LOG_DEBUG(&Logger::get("AWSClient"), "Here 5");

        Poco::Net::HTTPResponse response_;
        auto & response_body_stream = session->receiveResponse(response_);

        LOG_DEBUG(&Logger::get("AWSClient"), "Here 6");

        response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(response_.getStatus()));

        LOG_DEBUG(&Logger::get("AWSClient"), "Here 7");

        response->SetContentType(response_.getContentType());

        LOG_DEBUG(&Logger::get("AWSClient"), "Content length: {}", response_.getContentLength());

        LOG_DEBUG(&Logger::get("AWSClient"), "Received headers:");
        for (auto it = response_.begin(); it != response_.end(); ++it)
        {
            LOG_DEBUG(&Logger::get("AWSClient"), "{} : {}", it->first, it->second);
        }

        /// TODO: Do not copy whole stream.
        Poco::StreamCopier::copyStream(response_body_stream, response->GetResponseBody());

        if (response_.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
        {
            response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
            response->SetClientErrorMessage(response_.getReason());
        }
    }
    catch (...)
    {
        tryLogCurrentException(&Logger::get("AWSClient"), "Failed to make request");
        response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
        response->SetClientErrorMessage(getCurrentExceptionMessage(true));
    }
}
}
