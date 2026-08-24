#include <IO/GCPOAuth.h>

#include <fmt/format.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/HTTPCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
}

namespace
{

GCPOAuthToken postTokenRequest(
    const std::string & token_endpoint,
    const std::string & body,
    const ConnectionTimeouts & timeouts,
    HTTPConnectionGroupType group)
{
    Poco::URI url(token_endpoint);

    auto log = getLogger("GCPOAuth");
    LOG_DEBUG(log, "Requesting GCP bearer token from {}", url.getHost());

    HTTPSessionPtr session;
    std::exception_ptr last_exception;
    for (size_t i = 0; i < 5; ++i)
    {
        try
        {
            session = makeHTTPSession(group, url, timeouts);
            break;
        }
        catch (...)
        {
            last_exception = std::current_exception();
            tryLogCurrentException(log);
        }
    }
    if (!session)
        std::rethrow_exception(last_exception);

    Poco::Net::HTTPRequest request(
        Poco::Net::HTTPRequest::HTTP_POST,
        url.getPathAndQuery(),
        Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentType("application/x-www-form-urlencoded");
    request.setContentLength(body.size());
    request.set("Accept", "application/json");

    std::ostream & os = session->sendRequest(request);
    os << body;

    Poco::Net::HTTPResponse response;
    std::istream & rs = session->receiveResponse(response);

    String token_json_raw;
    Poco::StreamCopier::copyToString(rs, token_json_raw);

    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Failed to obtain GCP access token: {} (HTTP {})",
            response.getReason(),
            static_cast<int>(response.getStatus()));

    Poco::JSON::Parser parser;
    auto object = parser.parse(token_json_raw).extract<Poco::JSON::Object::Ptr>();

    if (!object->has("access_token") || !object->has("token_type"))
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Unexpected GCP token response: missing 'access_token' or 'token_type'");

    auto token_type = object->getValue<String>("token_type");
    if (token_type != "Bearer")
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Unexpected GCP token type: expected 'Bearer', got '{}'",
            token_type);

    GCPOAuthToken result;
    result.access_token = object->getValue<String>("access_token");
    if (object->has("expires_in"))
        result.expires_in = object->getValue<Int64>("expires_in");

    return result;
}

}

GCPOAuthToken fetchGCPOAuthToken(
    const std::string & client_id,
    const std::string & client_secret,
    const std::string & refresh_token,
    const ConnectionTimeouts & timeouts,
    HTTPConnectionGroupType group,
    const std::string & token_endpoint)
{
    std::string encoded_client_id;
    std::string encoded_client_secret;
    std::string encoded_refresh_token;
    Poco::URI::encode(client_id, "", encoded_client_id);
    Poco::URI::encode(client_secret, "", encoded_client_secret);
    Poco::URI::encode(refresh_token, "", encoded_refresh_token);

    String body = fmt::format(
        "grant_type=refresh_token&client_id={}&client_secret={}&refresh_token={}",
        encoded_client_id, encoded_client_secret, encoded_refresh_token);

    return postTokenRequest(token_endpoint, body, timeouts, group);
}

GCPOAuthToken fetchGCPOAuthTokenWithJWTAssertion(
    const std::string & assertion,
    const std::string & token_endpoint,
    const ConnectionTimeouts & timeouts,
    HTTPConnectionGroupType group)
{
    std::string encoded_assertion;
    Poco::URI::encode(assertion, "", encoded_assertion);

    String body = fmt::format(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={}",
        encoded_assertion);

    return postTokenRequest(token_endpoint, body, timeouts, group);
}

}
