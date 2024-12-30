#include <Disks/ObjectStorages/AzureBlobStorage/AzureDelegatedKeyPolicy.h>

#if USE_AZURE_BLOB_STORAGE

#include <Common/Exception.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

namespace DB {

std::string AzureDelegatedKeyPolicy::GetSignature(const std::string& string_to_sign) const
{
    Poco::URI url(signature_delegation_url);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, url.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    request.setHost(url.getHost());

    Poco::JSON::Object request_json;
    request_json.set("stringToSign", string_to_sign);
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
    catch (const Poco::Exception & e)
    {
        session->attachSessionData(e.message());
        throw Exception(Exception::CreateFromPocoTag{}, e);
    }

    try
    {
        Poco::JSON::Parser parser;
        auto response_json = parser.parse(response_body).extract<Poco::JSON::Object::Ptr>();
        return response_json->getValue<std::string>("signature");
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(Exception::CreateFromPocoTag{}, e);
    }
}

}

#endif
