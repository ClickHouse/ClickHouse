#include <config.h>

#if USE_JWT_CPP && USE_SSL
#include <Client/CloudJWTProvider.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/ErrorCodes.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/Dynamic/Var.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatSettings.h>

#include <thread>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <jwt-cpp/jwt.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
}

const std::map<std::string, CloudJWTProvider::AuthEndpoints> CloudJWTProvider::managed_service_endpoints = {
    {
        ".clickhouse-dev.com",
        {
            "https://auth.control-plane.clickhouse-dev.com",
            "dKv0XkTAw7rghGiAa5sjPFYGQUVtjzuz",
            "https://console-api-internal.clickhouse-dev.com"
        }
    },
    {
        ".clickhouse-staging.com",
        {
            "https://auth.control-plane.clickhouse-staging.com",
            "rpEkizLMmAU95MP4JL8ERefbVXtUQSFs",
            "https://console-api-internal.clickhouse-staging.com"
        }
    },
    {
        ".clickhouse.cloud",
        {
            "https://auth.clickhouse.cloud",
            "9Wf1YpSocOg5sp7GOcCjtrt6DWRAJ19S",
            "https://console-api-internal.clickhouse.cloud"
        }
    }
};

const CloudJWTProvider::AuthEndpoints * CloudJWTProvider::getAuthEndpoints(const std::string & host)
{
    for (const auto & [suffix, endpoints] : managed_service_endpoints)
    {
        if (endsWith(host, suffix))
            return &endpoints;
    }
    return nullptr;
}

CloudJWTProvider::CloudJWTProvider(
    std::string auth_url, std::string client_id, std::string audience, std::string host,
    std::ostream & out, std::ostream & err)
    : JWTProvider(std::move(auth_url), std::move(client_id), std::move(audience), out, err),
      host_str(std::move(host))
{
    if (oauth_url.empty() || oauth_client_id.empty())
    {
        if (const auto * endpoints = getAuthEndpoints(host_str))
        {
            if (oauth_url.empty())
                oauth_url = endpoints->auth_url;
            if (oauth_client_id.empty())
                oauth_client_id = endpoints->client_id;
        }
    }
}

std::string CloudJWTProvider::getJWT()
{
    Poco::Timestamp now;
    Poco::Timestamp expiration_buffer = 30 * Poco::Timespan::SECONDS;

    // If we have a valid ClickHouse JWT, return it.
    if (!clickhouse_jwt.empty() && now < clickhouse_jwt_expires_at - expiration_buffer)
        return clickhouse_jwt;

    // If we have a valid IDP refresh token, attempt to refresh the IDP access token if expired.
    if (!idp_refresh_token.empty() && now >= idp_access_token_expires_at - expiration_buffer)
    {
        refreshIdPAccessToken();
    }

    // If we have a valid IDP access token, attempt to exchange it for a ClickHouse JWT.
    if (!idp_access_token.empty() && now < idp_access_token_expires_at - expiration_buffer)
    {
        exchangeIdPTokenForClickHouseJWT(false);
        return clickhouse_jwt;
    }

    // If we don't have a valid ClickHouse JWT, attempt to login and exchange the IDP token for a ClickHouse JWT.
    deviceCodeLogin();
    exchangeIdPTokenForClickHouseJWT(true);
    return clickhouse_jwt;
}

std::string CloudJWTProvider::getAudience() const
{
    return "token-exchange";
}

void CloudJWTProvider::exchangeIdPTokenForClickHouseJWT(bool show_messages)
{
    const auto * endpoints = getAuthEndpoints(host_str);

    if (!endpoints)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot determine token exchange endpoint from hostname {}. Please provide a ClickHouse Cloud hostname.",
            host_str);

    Poco::URI exchange_url = Poco::URI(endpoints->api_host + "/.api/auth/tokenExchange");

    if (show_messages)
        output_stream << "Authenticating access to " << host_str << "." << std::endl;

    auto session = createHTTPSession(exchange_url);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, exchange_url.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    request.set("Authorization", "Bearer " + idp_access_token);
    request.setContentType("application/json; charset=utf-8");

    std::string request_body;
    WriteBufferFromString request_payload_buffer(request_body);
    writeCString(R"({"hostname": )", request_payload_buffer);
    writeJSONString(host_str, request_payload_buffer, {});
    writeCString(R"(})", request_payload_buffer);
    request_payload_buffer.finalize();

    request.setContentLength(request_body.length());
    session->sendRequest(request) << request_body;

    Poco::Net::HTTPResponse response;
    std::istream & rs = session->receiveResponse(response);
    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        std::string error_body;
        Poco::StreamCopier::copyToString(rs, error_body);
        throw Exception(ErrorCodes::NETWORK_ERROR, "Error exchanging token: {} {}\nResponse: {}", response.getStatus(), response.getReason(), error_body);
    }

    std::string response_body;
    Poco::StreamCopier::copyToString(rs, response_body);

    Poco::JSON::Object::Ptr object = Poco::JSON::Parser().parse(response_body).extract<Poco::JSON::Object::Ptr>();
    clickhouse_jwt = object->getValue<std::string>("token");
    clickhouse_jwt_expires_at = Poco::Timestamp::fromEpochTime(jwt::decode(clickhouse_jwt).get_payload_claim("exp").as_integer());

    if (show_messages)
        output_stream << "Authenticated with ClickHouse Cloud.\n" << std::endl;
}

}
#endif
