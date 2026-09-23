#include <IO/GCPOAuth.h>

#include "config.h"

#include <chrono>
#include <sstream>
#include <fmt/format.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/HTTPCommon.h>

#if USE_SSL
#    include <Common/Crypto/KeyPair.h>
#    include <Common/OpenSSLHelpers.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

constexpr auto GOOGLE_OAUTH2_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token";

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

GCPServiceAccountAssertion makeGCPServiceAccountAssertion(
    const std::string & service_account_key,
    const std::string & scope,
    const std::string & token_endpoint_override)
{
    Poco::JSON::Object::Ptr key_object;
    try
    {
        Poco::JSON::Parser parser;
        key_object = parser.parse(service_account_key).extract<Poco::JSON::Object::Ptr>();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse Google service account key: {}", e.displayText());
    }

    if (!key_object || !key_object->has("client_email") || !key_object->has("private_key"))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Google service account key must be a JSON object with 'client_email' and 'private_key' "
            "(the content of a key file downloaded from Google Cloud IAM)");

    const auto client_email = key_object->getValue<String>("client_email");
    const auto private_key = key_object->getValue<String>("private_key");
    String token_endpoint = GOOGLE_OAUTH2_TOKEN_ENDPOINT;
    if (key_object->has("token_uri"))
        token_endpoint = key_object->getValue<String>("token_uri");
    if (!token_endpoint_override.empty())
        token_endpoint = token_endpoint_override;

#if USE_SSL
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    Poco::JSON::Object claims;
    claims.set("iss", client_email);
    claims.set("scope", scope);
    claims.set("aud", token_endpoint);
    claims.set("iat", now);
    claims.set("exp", now + 3600);

    std::ostringstream claims_stream;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    claims.stringify(claims_stream);

    static constexpr auto header = R"({"alg":"RS256","typ":"JWT"})";
    String to_sign = fmt::format(
        "{}.{}",
        base64Encode(header, /*url_encoding*/ true, /*no_padding*/ true),
        base64Encode(claims_stream.str(), /*url_encoding*/ true, /*no_padding*/ true));

    auto key_pair = KeyPair::fromPEMString(private_key);
    String signature = rsaSHA256Sign(static_cast<EVP_PKEY *>(key_pair), to_sign);

    String assertion = fmt::format("{}.{}", to_sign, base64Encode(signature, /*url_encoding*/ true, /*no_padding*/ true));
    return {std::move(assertion), std::move(token_endpoint)};
#else
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED,
        "Authentication with a Google service account key requires ClickHouse to be built with SSL support");
#endif
}

}
