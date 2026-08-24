#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <Databases/DataLake/AWSV4Signer.h>

#include <Common/Exception.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/String.h>

#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/memory/AWSMemory.h>

#include <sstream>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int S3_ERROR;
}
}

namespace DataLake
{
namespace
{

Aws::Http::HttpMethod mapPocoMethodToAws(const String & method)
{
    using Aws::Http::HttpMethod;
    using Poco::Net::HTTPRequest;

    static const std::pair<String, HttpMethod> supported_methods[] = {
        {HTTPRequest::HTTP_GET, HttpMethod::HTTP_GET},
        {HTTPRequest::HTTP_POST, HttpMethod::HTTP_POST},
        {HTTPRequest::HTTP_PUT, HttpMethod::HTTP_PUT},
        {HTTPRequest::HTTP_DELETE, HttpMethod::HTTP_DELETE},
        {HTTPRequest::HTTP_HEAD, HttpMethod::HTTP_HEAD},
        {HTTPRequest::HTTP_PATCH, HttpMethod::HTTP_PATCH},
    };

    for (const auto & [poco_method, aws_method] : supported_methods)
        if (method == poco_method)
            return aws_method;

    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported HTTP method for AWS SigV4 signing: {}", method);
}

}

void signRequestWithAWSV4(
    const String & method,
    const Poco::URI & uri,
    const DB::HTTPHeaderEntries & extra_headers,
    const String & payload,
    Aws::Client::AWSAuthV4Signer & signer,
    const String & region,
    const String & service,
    DB::HTTPHeaderEntries & out_headers)
{
    const Aws::Http::URI aws_uri(uri.toString().c_str());
    Aws::Http::Standard::StandardHttpRequest request(aws_uri, mapPocoMethodToAws(method));

    for (const auto & h : extra_headers)
    {
        if (Poco::icompare(h.name, "authorization") == 0)
            continue;
        request.SetHeaderValue(Aws::String(h.name.c_str(), h.name.size()), Aws::String(h.value.c_str(), h.value.size()));
    }

    if (!payload.empty())
    {
        auto body_stream = Aws::MakeShared<std::stringstream>("AWSV4Signer");
        body_stream->write(payload.data(), static_cast<std::streamsize>(payload.size()));
        body_stream->seekg(0);
        request.AddContentBody(body_stream);
    }

    static constexpr bool sign_body = true;
    if (!signer.SignRequest(request, region.c_str(), service.c_str(), sign_body))
        throw DB::Exception(DB::ErrorCodes::S3_ERROR, "AWS SigV4 signing failed");

    bool has_authorization = false;
    for (const auto & [key, value] : request.GetHeaders())
    {
        if (Poco::icompare(key, "authorization") == 0 && !value.empty())
            has_authorization = true;
    }
    if (!has_authorization)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "AWS credentials are missing or incomplete; cannot sign S3 Tables REST request");

    out_headers.clear();
    for (const auto & [key, value] : request.GetHeaders())
    {
        if (Poco::icompare(key, "host") == 0)
            continue;
        out_headers.emplace_back(String(key.c_str(), key.size()), String(value.c_str(), value.size()));
    }
}

}

#endif
