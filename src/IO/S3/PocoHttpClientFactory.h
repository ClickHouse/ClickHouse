#pragma once

#include <aws/core/http/HttpClientFactory.h>

namespace Aws::Http
{
    class HttpClient;
    class HttpRequest;
}

namespace DB::S3
{

class PocoHttpClientFactory : public Aws::Http::HttpClientFactory
{
public:
    ~PocoHttpClientFactory() override = default;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration & clientConfiguration) const override;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(const Aws::String & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const override;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(const Aws::Http::URI & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const override;
};

}
