#pragma once

#include <Common/RemoteHostFilter.h>
#include <IO/ConnectionTimeouts.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>

namespace Aws::Http::Standard
{
class StandardHttpResponse;
}

namespace DB
{
class Context;
}

namespace DB::S3
{

struct PocoHTTPClientConfiguration : public Aws::Client::ClientConfiguration
{
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects;

    PocoHTTPClientConfiguration(const Aws::Client::ClientConfiguration & cfg, const RemoteHostFilter & remote_host_filter_,
        unsigned int s3_max_redirects_);

    void updateSchemeAndRegion();
};

class PocoHTTPClient : public Aws::Http::HttpClient
{
public:
    explicit PocoHTTPClient(const PocoHTTPClientConfiguration & clientConfiguration);
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
    void makeRequestInternal(
        Aws::Http::HttpRequest & request,
        std::shared_ptr<Aws::Http::Standard::StandardHttpResponse> & response,
        Aws::Utils::RateLimits::RateLimiterInterface * readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface * writeLimiter) const;

    std::function<Aws::Client::ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)> per_request_configuration;
    ConnectionTimeouts timeouts;
    const RemoteHostFilter & remote_host_filter;
    unsigned int s3_max_redirects;
};

}
