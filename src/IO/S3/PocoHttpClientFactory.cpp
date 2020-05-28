#include "PocoHttpClientFactory.h"

#include <IO/S3/PocoHttpClient.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

namespace DB::S3
{
std::shared_ptr<Aws::Http::HttpClient>
PocoHttpClientFactory::CreateHttpClient(const Aws::Client::ClientConfiguration & clientConfiguration) const
{
    return std::make_shared<PocoHttpClient>(clientConfiguration);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHttpClientFactory::CreateHttpRequest(
    const Aws::String & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const
{
    return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHttpClientFactory::CreateHttpRequest(
    const Aws::Http::URI & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const
{
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("PocoHttpClientFactory", uri, method);
    request->SetResponseStreamFactory(streamFactory);

    return request;
}

}
