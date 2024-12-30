#include "AWSAuthV4DelegatedSigner.h"

#if USE_AWS_S3

#include <aws/core/auth/AWSCredentials.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/StreamCopier.h>

namespace DB::S3
{

AWSAuthV4DelegatedSigner::AWSAuthV4DelegatedSigner(
    const Aws::String & signatureDelegationUrl,
    const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentialsProvider,
    const char * serviceName,
    const Aws::String & region,
    PayloadSigningPolicy signingPolicy,
    bool urlEscapePath,
    Aws::Auth::AWSSigningAlgorithm signingAlgorithm)
    : Aws::Client::AWSAuthV4Signer(credentialsProvider, serviceName, region, signingPolicy, urlEscapePath, signingAlgorithm)
    , signature_delegation_url(signatureDelegationUrl)
    , logger(&Poco::Logger::get("AWSAuthV4DelegatedSigner"))
{
}

Aws::String AWSAuthV4DelegatedSigner::GenerateSignature(
    const Aws::String & canonicalRequestString,
    const Aws::String & date,
    const Aws::String & simpleDate,
    const Aws::String & signingRegion,
    const Aws::String & signingServiceName,
    const Aws::Auth::AWSCredentials & credentials) const
{
    if (signature_delegation_url.empty())
    {
        return Aws::Client::AWSAuthV4Signer::GenerateSignature(
            canonicalRequestString, date, simpleDate, signingRegion, signingServiceName, credentials
        );
    }
    Poco::URI url(signature_delegation_url);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, url.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    request.setHost(url.getHost());

    Poco::JSON::Object request_json;
    request_json.set("canonicalRequest", canonicalRequestString);
    std::ostringstream request_string_stream;
    request_string_stream.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(request_json, request_string_stream);
    request.setContentLength(request_string_stream.tellp());
    request.setContentType("application/json");

    auto timeouts = ConnectionTimeouts()
        .withConnectionTimeout(30)
        .withSendTimeout(30)
        .withReceiveTimeout(30)
        // Since the proxy is on the same host, we don't need to keep the
        // connection alive. The OS proxy has a low keep-alive timeout, if the
        // server closes the connection early we can get NoMessageException
        // errors.
        .withHTTPKeepAliveTimeout(0);

    std::string response_body;
    auto session = makeHTTPSession(HTTPConnectionGroupType::DISK, url, timeouts);
    try
    {
        auto & request_body_stream = session->sendRequest(request);
        request_body_stream << request_string_stream.str();
        Poco::Net::HTTPResponse response;
        auto & response_body_stream = session->receiveResponse(response);
        assertResponseIsOk(request.getURI(), response, response_body_stream, /* allow_redirects= */ false);
        Poco::StreamCopier::copyToString(response_body_stream, response_body);
    }
    catch (const Poco::Exception &)
    {
        auto message = getCurrentExceptionMessage(true);
        LOG_ERROR(logger, "Request for signature delegation failed.  Error: {}", message);
        return "";
    }

    try
    {
        Poco::JSON::Parser parser;
        auto response_json = parser.parse(response_body).extract<Poco::JSON::Object::Ptr>();
        return response_json->getValue<std::string>("signature");
    }
    catch (const Poco::Exception &)
    {
        auto message = getCurrentExceptionMessage(true);
        LOG_ERROR(logger, "Parsing signature delegation response failed.  Error {}", message);
        return "";
    }
}

}

#endif
