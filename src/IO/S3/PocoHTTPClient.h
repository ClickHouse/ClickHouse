#pragma once

#include <IO/ConnectionTimeouts.h>
#include <aws/core/http/HttpClient.h>

namespace Aws::Http::Standard
{
class StandardHttpResponse;
}

namespace DB::S3
{

class PocoHTTPClient : public Aws::Http::HttpClient
{
public:
    explicit PocoHTTPClient(const Aws::Client::ClientConfiguration & clientConfiguration);
    ~PocoHTTPClient() override = default;
    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
        Aws::Http::HttpRequest & request,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const override;

    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest> & request,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const override;

private:
    void MakeRequestInternal(
        Aws::Http::HttpRequest & request,
        std::shared_ptr<Aws::Http::Standard::StandardHttpResponse> & response,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const;

    std::function<Aws::Client::ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)> per_request_configuration;
    ConnectionTimeouts timeouts;
};

}
