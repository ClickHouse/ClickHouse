#pragma once

#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

namespace Aws::Http
{
class HttpClient;
class HttpRequest;
}

namespace DB::S3
{

/// The typed HTTP request every `PocoHTTPClientFactory::CreateHttpRequest` overload constructs,
/// carrying the `NativeConditional` bit `Client::BuildHttpRequest` derives on every SDK attempt from
/// the operation wrapper's `RequestWithNativeConditionalMode::isNativeConditional`. A request
/// reaching `PocoHTTPClient` through any other path (e.g. built directly in a test) is foreign and
/// reads as `Default` via `isNativeConditionalRequest`.
class ExtendedHttpRequest final : public Aws::Http::Standard::StandardHttpRequest
{
public:
    using StandardHttpRequest::StandardHttpRequest;

    void setNativeConditional(bool value = true) { native_conditional = value; }
    bool isNativeConditional() const { return native_conditional; }

private:
    bool native_conditional = false;
};

/// False for a foreign `Aws::Http::HttpRequest` that isn't an `ExtendedHttpRequest`.
bool isNativeConditionalRequest(const Aws::Http::HttpRequest & request) noexcept;

class PocoHTTPClientFactory : public Aws::Http::HttpClientFactory
{
public:
    ~PocoHTTPClientFactory() override = default;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpClient>
    CreateHttpClient(const Aws::Client::ClientConfiguration & client_configuration) const override;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(const Aws::String & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const override;
    [[nodiscard]] std::shared_ptr<Aws::Http::HttpRequest>
    CreateHttpRequest(const Aws::Http::URI & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const override;

private:
    const Aws::IOStreamFactory null_factory = []() { return nullptr; };
};

}
