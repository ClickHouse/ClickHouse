#include "config.h"

#if USE_AWS_S3

#include "PocoHTTPClientFactory.h"

#include <IO/S3/PocoHTTPClient.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

namespace DB::S3
{
std::shared_ptr<Aws::Http::HttpClient>
PocoHTTPClientFactory::CreateHttpClient(const Aws::Client::ClientConfiguration & client_configuration) const
{
    if (client_configuration.userAgent.starts_with("ClickHouse"))
        return std::make_shared<PocoHTTPClient>(static_cast<const PocoHTTPClientConfiguration &>(client_configuration));
    /// This client is created inside the AWS SDK with default settings to obtain ECS credentials from localhost.
    return std::make_shared<PocoHTTPClient>(client_configuration);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHTTPClientFactory::CreateHttpRequest(
    const Aws::String & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const
{
    return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHTTPClientFactory::CreateHttpRequest(
    const Aws::Http::URI & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory &) const
{
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("PocoHTTPClientFactory", uri, method);

    /// Don't create default response stream. Actual response stream will be set later in PocoHTTPClient.
    request->SetResponseStreamFactory(null_factory);

    return request;
}

}

#endif
