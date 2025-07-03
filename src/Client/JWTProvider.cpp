#include <config.h>

#if USE_JWT_CPP && USE_SSL
#include <Client/JWTProvider.h>
#include <Common/Exception.h>

#include <Client/CloudJWTProvider.h>
#include <Common/StringUtils.h>
#include <Common/ErrorCodes.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/SSLManager.h>

#include <jwt-cpp/jwt.h>

#include <thread>
#include <chrono>
#if defined(OS_DARWIN) || defined(OS_LINUX)
#include <spawn.h>
#include <sys/wait.h>
#elif defined(OS_WINDOWS)
#include <windows.h>
#include <shellapi.h>
#endif
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
    extern const int AUTHENTICATION_FAILED;
    extern const int TIMEOUT_EXCEEDED;
}

JWTProvider::JWTProvider(
    std::string auth_url,
    std::string client_id,
    std::ostream & out,
    std::ostream & err)
    : auth_url_str(std::move(auth_url)),
      client_id_str(std::move(client_id)),
      output_stream(out),
      error_stream(err)
{
}

std::string JWTProvider::getJWT()
{
    Poco::Timestamp now;
    Poco::Timestamp expiration_buffer = 15 * Poco::Timespan::SECONDS;

    if (!idp_access_token.empty() && now < idp_access_token_expires_at - expiration_buffer)
        return idp_access_token;

    if (!idp_refresh_token.empty())
    {
        refreshIdPAccessToken();
        return idp_access_token;
    }

    deviceCodeLogin();
    return idp_access_token;
}

void JWTProvider::deviceCodeLogin()
{
    if (client_id_str.empty() || auth_url_str.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error: --auth-client-id and --auth-url are required for --login.");

    Poco::URI device_code_url(auth_url_str + "/oauth/device/code");
    Poco::URI token_url(auth_url_str + "/oauth/token");

    std::string scope = "openid profile email offline_access";
    std::string audience = "control-plane-web";

    std::string device_code;
    int interval_seconds = 5;
    Poco::Timestamp::TimeVal expires_at_ts = 0;

    auto device_code_session = createHTTPSession(device_code_url);
    Poco::Net::HTTPRequest device_code_request(Poco::Net::HTTPRequest::HTTP_POST, device_code_url.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    device_code_request.setContentType("application/x-www-form-urlencoded");

    std::string encoded_scope;
    Poco::URI::encode(scope, "", encoded_scope);
    std::string encoded_audience;
    Poco::URI::encode(audience, "", encoded_audience);
    std::string device_code_request_body = "client_id=" + client_id_str + "&scope=" + encoded_scope + "&audience=" + encoded_audience;

    device_code_request.setContentLength(device_code_request_body.length());
    device_code_session->sendRequest(device_code_request) << device_code_request_body;

    Poco::Net::HTTPResponse device_code_response;
    std::istream & device_code_rs = device_code_session->receiveResponse(device_code_response);
    if (device_code_response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        std::string error_body;
        Poco::StreamCopier::copyToString(device_code_rs, error_body);
        throw Exception(ErrorCodes::NETWORK_ERROR, "Error requesting device code: {} {}\nResponse: {}", device_code_response.getStatus(), device_code_response.getReason(), error_body);
    }

    Poco::JSON::Object::Ptr device_code_object = Poco::JSON::Parser().parse(device_code_rs).extract<Poco::JSON::Object::Ptr>();
    device_code = device_code_object->getValue<std::string>("device_code");
    std::string user_code = device_code_object->getValue<std::string>("user_code");
    std::string verification_uri_complete = device_code_object->getValue<std::string>("verification_uri_complete");
    interval_seconds = device_code_object->getValue<int>("interval");
    expires_at_ts = Poco::Timestamp().epochTime() + device_code_object->getValue<int>("expires_in");

    output_stream << "\nOpening your browser for login. If it doesn't open, visit:"
                    << "\n\n        " << verification_uri_complete << "\n\n"
                    << "Then enter the code: \033[1m" << user_code << "\033[0m\n" << std::endl;

    openURLInBrowser(verification_uri_complete);

    while (Poco::Timestamp().epochTime() < expires_at_ts)
    {
        std::this_thread::sleep_for(std::chrono::seconds(interval_seconds));

        auto token_session = createHTTPSession(token_url);
        Poco::Net::HTTPRequest token_request(Poco::Net::HTTPRequest::HTTP_POST, token_url.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
        token_request.setContentType("application/x-www-form-urlencoded");
        std::string token_request_body = "grant_type=urn:ietf:params:oauth:grant-type:device_code&device_code=" + device_code + "&client_id=" + client_id_str;
        token_request.setContentLength(token_request_body.length());
        token_session->sendRequest(token_request) << token_request_body;

        Poco::Net::HTTPResponse token_response;
        std::istream & token_rs = token_session->receiveResponse(token_response);
        std::string response_body;
        Poco::StreamCopier::copyToString(token_rs, response_body);
        Poco::JSON::Object::Ptr token_object = Poco::JSON::Parser().parse(response_body).extract<Poco::JSON::Object::Ptr>();

        if (token_response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
        {
            idp_access_token = token_object->getValue<std::string>("access_token");
            idp_access_token_expires_at = Poco::Timestamp::fromEpochTime(jwt::decode(idp_access_token).get_payload_claim("exp").as_integer());
            if (token_object->has("refresh_token"))
                idp_refresh_token = token_object->getValue<std::string>("refresh_token");
            return;
        }

        std::string error = token_object->optValue<std::string>("error", "unknown_error");
        if (error != "authorization_pending" && error != "slow_down")
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "IdP login failed: {}", token_object->optValue<std::string>("error_description", error));
        }
    }

    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Device login flow timed out.");
}

void JWTProvider::refreshIdPAccessToken()
{
    Poco::URI token_url(auth_url_str + "/oauth/token");

    auto session = createHTTPSession(token_url);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, token_url.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentType("application/x-www-form-urlencoded");

    std::string request_body = "grant_type=refresh_token&client_id=" + client_id_str + "&refresh_token=" + idp_refresh_token;
    request.setContentLength(request_body.length());
    session->sendRequest(request) << request_body;

    Poco::Net::HTTPResponse response;
    std::istream & rs = session->receiveResponse(response);
    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        std::string error_body;
        Poco::StreamCopier::copyToString(rs, error_body);
        idp_refresh_token.clear();
        throw Exception(ErrorCodes::NETWORK_ERROR, "Error refreshing token: {} {}\nResponse: {}", response.getStatus(), response.getReason(), error_body);
    }

    Poco::JSON::Object::Ptr object = Poco::JSON::Parser().parse(rs).extract<Poco::JSON::Object::Ptr>();
    idp_access_token = object->getValue<std::string>("access_token");
    idp_access_token_expires_at = Poco::Timestamp::fromEpochTime(jwt::decode(idp_access_token).get_payload_claim("exp").as_integer());
    if (object->has("refresh_token"))
            idp_refresh_token = object->getValue<std::string>("refresh_token");
}

std::unique_ptr<Poco::Net::HTTPSClientSession> JWTProvider::createHTTPSession(const Poco::URI & uri)
{
    if (uri.getScheme() == "https")
    {
        Poco::Net::Context::Ptr context = Poco::Net::SSLManager::instance().defaultClientContext();
        return std::make_unique<Poco::Net::HTTPSClientSession>(uri.getHost(), uri.getPort(), context);
    }
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Built without SSL, ClickHouse cannot use JWT authentication without SSL support.");
}

void JWTProvider::openURLInBrowser(const std::string & url)
{
    if (url.empty())
        return;

#if defined(OS_DARWIN) || defined(OS_LINUX)
    std::string command;
#if defined(OS_DARWIN)
    command = "open";
#elif defined(OS_LINUX)
    command = "xdg-open";
#endif

    if (command.empty())
        return;

    pid_t pid;
    const char * argv[] = {command.c_str(), url.c_str(), nullptr};
    int status = posix_spawnp(&pid, command.c_str(), nullptr, nullptr, const_cast<char * const *>(argv), nullptr);

    if (status == 0)
    {
        int wait_status;
        waitpid(pid, &wait_status, 0);
    }
#elif defined(OS_WINDOWS)
    ShellExecuteA(NULL, "open", url.c_str(), NULL, NULL, SW_SHOWNORMAL);
#endif
}

Poco::Timestamp JWTProvider::getJwtExpiry(const std::string & token)
{
    if (token.empty())
        return 0;

    try
    {
        auto decoded_token = jwt::decode(token);
        return Poco::Timestamp::fromEpochTime(decoded_token.get_payload_claim("exp").as_integer());
    }
    catch (...)
    {
        return 0;
    }
}

bool isCloudEndpoint(const std::string & host)
{
    return endsWith(host, ".clickhouse.cloud") || endsWith(host, ".clickhouse-staging.com") || endsWith(host, ".clickhouse-dev.com");
}

std::unique_ptr<JWTProvider> createJwtProvider(
    const std::string & auth_url,
    const std::string & client_id,
    const std::string & host,
    std::ostream & out,
    std::ostream & err)
{
    if (isCloudEndpoint(host))
    {
        return std::make_unique<CloudJWTProvider>(auth_url, client_id, host, out, err);
    }
    else
    {
        return std::make_unique<JWTProvider>(auth_url, client_id, out, err);
    }
}

}

#endif
