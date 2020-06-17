#pragma once

#include <aws/core/http/HttpClientFactory.h>

namespace Aws::Http
{
class HttpClient;
class HttpRequest;
}

namespace DB::S3
{
class PocoHTTPClientFactory : public Aws::Http::HttpClientFactory
{
public:
    ~PocoHTTPClientFactory() override = default;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpClient>
    CreateHttpClient(const Aws::Client::ClientConfiguration & clientConfiguration) const override;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(const Aws::String & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const override;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(const Aws::Http::URI & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const override;

private:
    const Aws::IOStreamFactory null_factory = []() { return nullptr; };
};

}
