#include "PocoHTTPClientFactory.h"

#include <IO/S3/PocoHTTPClient.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

namespace DB::S3
{
std::shared_ptr<Aws::Http::HttpClient>
PocoHTTPClientFactory::CreateHttpClient(const Aws::Client::ClientConfiguration & clientConfiguration) const
{
    return std::make_shared<PocoHTTPClient>(clientConfiguration);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHTTPClientFactory::CreateHttpRequest(
    const Aws::String & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const
{
    return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHTTPClientFactory::CreateHttpRequest(
    const Aws::Http::URI & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const
{
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("PocoHTTPClientFactory", uri, method);
    request->SetResponseStreamFactory(streamFactory);

    return request;
}

}
