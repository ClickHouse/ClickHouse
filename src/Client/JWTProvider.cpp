#include <config.h>

#if USE_JWT_CPP && USE_SSL
#include <Client/JWTProvider.h>
#include <Client/OAuthDeviceFlow.h>
#include <Common/Exception.h>

#include <Client/CloudJWTProvider.h>
#include <Common/StringUtils.h>
#include <Common/ErrorCodes.h>
#include <Client/ClientBaseHelpers.h>

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

#include <chrono>
#include <iostream>
#include <thread>
#if defined(OS_DARWIN) || defined(OS_LINUX)
#include <spawn.h>
#include <sys/wait.h>
#elif defined(OS_WINDOWS)
#include <windows.h>
#include <shellapi.h>
#endif


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

void JWTProvider::storeAccessTokenFromResponse(const Poco::JSON::Object::Ptr & token_object)
{
    idp_access_token = token_object->getValue<std::string>("access_token");

    if (token_object->has("expires_in"))
    {
        idp_access_token_expires_at = Poco::Timestamp() + Poco::Timespan(token_object->getValue<int>("expires_in"), 0);
    }
    else
    {
        idp_access_token_expires_at = getJwtExpiry(idp_access_token);
    }

    if (token_object->has("refresh_token"))
        idp_refresh_token = token_object->getValue<std::string>("refresh_token");
}

JWTProvider::JWTProvider(
    JWTProviderOptions options,
    std::ostream & out,
    std::ostream & err)
    : oauth_url(normalizeOAuthIssuerURL(std::move(options.auth_url)))
    , oauth_client_id(std::move(options.client_id))
    , oauth_audience(std::move(options.audience))
    , oauth_scope(std::move(options.scope))
    , oauth_device_authorization_endpoint_override(std::move(options.device_authorization_endpoint))
    , oauth_token_endpoint_override(std::move(options.token_endpoint))
    , output_stream(out)
    , error_stream(err)
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

void JWTProvider::ensureOAuthEndpointsResolved()
{
    if (oauth_endpoints_resolved)
        return;

    const bool has_device_override = !oauth_device_authorization_endpoint_override.empty();
    const bool has_token_override = !oauth_token_endpoint_override.empty();

    if (has_device_override != has_token_override)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Both --oauth-device-uri and --oauth-token-uri must be provided together, or omit both to use OIDC discovery / Auth0-style paths from --oauth-url");
    }

    if (has_device_override && has_token_override)
    {
        resolved_device_authorization_endpoint = oauth_device_authorization_endpoint_override;
        resolved_token_endpoint = oauth_token_endpoint_override;
        oauth_endpoints_resolved = true;
        return;
    }

    if (oauth_url.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Error: --oauth-url is required for --login unless both --oauth-device-uri and --oauth-token-uri are set.");
    }

    std::optional<OAuthDeviceFlowEndpoints> discovered;
    std::string last_error;

    for (const auto & discovery_url : buildOAuthDiscoveryURLs(oauth_url))
    {
        try
        {
            Poco::URI uri(discovery_url);
            const std::string body = httpGet(uri);
            discovered = parseOAuthDiscoveryDocument(body);
            if (discovered)
                break;
            last_error = "discovery document at " + discovery_url
                + " is missing device_authorization_endpoint or token_endpoint";
        }
        catch (const Exception & e)
        {
            last_error = discovery_url + ": " + e.message();
        }
        catch (...)
        {
            last_error = discovery_url + ": " + getCurrentExceptionMessage(false);
        }
    }

    OAuthDeviceFlowEndpoints endpoints = discovered ? *discovered : auth0StyleOAuthEndpoints(oauth_url);
    if (!discovered && !last_error.empty())
    {
        /// Discovery failed; keep Auth0-compatible fallback for existing ClickHouse Cloud / Auth0 users.
        error_stream << "Warning: OAuth discovery failed (" << last_error
                      << "); falling back to Auth0-style endpoints under " << oauth_url << std::endl;
    }

    endpoints = applyOAuthEndpointOverrides(
        std::move(endpoints),
        oauth_device_authorization_endpoint_override,
        oauth_token_endpoint_override);

    resolved_device_authorization_endpoint = endpoints.device_authorization_endpoint;
    resolved_token_endpoint = endpoints.token_endpoint;
    oauth_endpoints_resolved = true;
}

void JWTProvider::deviceCodeLogin()
{
    if (oauth_client_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error: --oauth-client-id is required for --login.");

    ensureOAuthEndpointsResolved();

    Poco::URI device_code_url(resolved_device_authorization_endpoint);
    Poco::URI token_url(resolved_token_endpoint);

    const std::string scope = oauth_scope.empty() ? default_oauth_device_code_scope : oauth_scope;
    const std::string audience = getAudience();

    auto device_code_session = createHTTPSession(device_code_url);
    Poco::Net::HTTPRequest device_code_request(
        Poco::Net::HTTPRequest::HTTP_POST, device_code_url.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    device_code_request.setContentType("application/x-www-form-urlencoded");

    std::string encoded_scope;
    Poco::URI::encode(scope, "", encoded_scope);
    std::string device_code_request_body = "client_id=" + oauth_client_id + "&scope=" + encoded_scope;

    if (!audience.empty())
    {
        std::string encoded_audience;
        Poco::URI::encode(audience, "", encoded_audience);
        device_code_request_body += "&audience=" + encoded_audience;
    }

    device_code_request.setContentLength(device_code_request_body.length());
    device_code_session->sendRequest(device_code_request) << device_code_request_body;

    Poco::Net::HTTPResponse device_code_response;
    std::istream & device_code_rs = device_code_session->receiveResponse(device_code_response);
    if (device_code_response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        std::string error_body;
        Poco::StreamCopier::copyToString(device_code_rs, error_body);
        throw Exception(
            ErrorCodes::NETWORK_ERROR,
            "Error requesting device code: {} {}\nResponse: {}",
            device_code_response.getStatus(),
            device_code_response.getReason(),
            error_body);
    }

    Poco::JSON::Object::Ptr device_code_object = Poco::JSON::Parser().parse(device_code_rs).extract<Poco::JSON::Object::Ptr>();
    const std::string device_code = device_code_object->getValue<std::string>("device_code");
    const std::string user_code = device_code_object->getValue<std::string>("user_code");
    const std::string verification_uri_complete = device_code_object->optValue<std::string>("verification_uri_complete", "");
    const std::string verification_uri = device_code_object->optValue<std::string>("verification_uri", "");
    const std::string verification_url = resolveDeviceVerificationURI(verification_uri_complete, verification_uri, user_code);
    int interval_seconds = device_code_object->optValue<int>("interval", 5);
    const Poco::Timestamp::TimeVal expires_at_ts
        = Poco::Timestamp().epochTime() + device_code_object->getValue<int>("expires_in");

    output_stream << "\nOpening your browser for login. If it doesn't open, visit:"
                   << "\n\n        " << verification_url << "\n\n"
                   << "Then enter the code: \033[1m" << user_code << "\033[0m\n"
                   << std::endl;

    openURLInBrowser(verification_url);

    std::string encoded_device_code;
    Poco::URI::encode(device_code, "", encoded_device_code);

    while (Poco::Timestamp().epochTime() < expires_at_ts)
    {
        std::this_thread::sleep_for(std::chrono::seconds(interval_seconds));

        auto token_session = createHTTPSession(token_url);
        Poco::Net::HTTPRequest token_request(
            Poco::Net::HTTPRequest::HTTP_POST, token_url.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
        token_request.setContentType("application/x-www-form-urlencoded");
        const std::string token_request_body
            = "grant_type=urn:ietf:params:oauth:grant-type:device_code&device_code=" + encoded_device_code
            + "&client_id=" + oauth_client_id;
        token_request.setContentLength(token_request_body.length());
        token_session->sendRequest(token_request) << token_request_body;

        Poco::Net::HTTPResponse token_response;
        std::istream & token_rs = token_session->receiveResponse(token_response);
        std::string response_body;
        Poco::StreamCopier::copyToString(token_rs, response_body);
        Poco::JSON::Object::Ptr token_object = Poco::JSON::Parser().parse(response_body).extract<Poco::JSON::Object::Ptr>();

        if (token_response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
        {
            storeAccessTokenFromResponse(token_object);
            return;
        }

        const std::string error = token_object->optValue<std::string>("error", "unknown_error");
        if (error == "slow_down")
        {
            /// RFC 8628: increase the polling interval by 5 seconds on slow_down.
            interval_seconds += 5;
            continue;
        }
        if (error != "authorization_pending")
        {
            throw Exception(
                ErrorCodes::AUTHENTICATION_FAILED,
                "IdP login failed: {}",
                token_object->optValue<std::string>("error_description", error));
        }
    }

    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Device login timed out.");
}

void JWTProvider::refreshIdPAccessToken()
{
    ensureOAuthEndpointsResolved();

    Poco::URI token_url(resolved_token_endpoint);

    auto session = createHTTPSession(token_url);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, token_url.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentType("application/x-www-form-urlencoded");

    std::string encoded_refresh_token;
    Poco::URI::encode(idp_refresh_token, "", encoded_refresh_token);
    const std::string request_body
        = "grant_type=refresh_token&client_id=" + oauth_client_id + "&refresh_token=" + encoded_refresh_token;
    request.setContentLength(request_body.length());
    session->sendRequest(request) << request_body;

    Poco::Net::HTTPResponse response;
    std::istream & rs = session->receiveResponse(response);
    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        std::string error_body;
        Poco::StreamCopier::copyToString(rs, error_body);
        idp_refresh_token.clear();
        throw Exception(
            ErrorCodes::NETWORK_ERROR,
            "Error refreshing token: {} {}\nResponse: {}",
            response.getStatus(),
            response.getReason(),
            error_body);
    }

    Poco::JSON::Object::Ptr object = Poco::JSON::Parser().parse(rs).extract<Poco::JSON::Object::Ptr>();
    storeAccessTokenFromResponse(object);
}

std::string JWTProvider::httpGet(const Poco::URI & uri)
{
    auto session = createHTTPSession(uri);
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPMessage::HTTP_1_1);
    request.set("Accept", "application/json");
    session->sendRequest(request);

    Poco::Net::HTTPResponse response;
    std::istream & rs = session->receiveResponse(response);
    std::string body;
    Poco::StreamCopier::copyToString(rs, body);

    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        throw Exception(
            ErrorCodes::NETWORK_ERROR,
            "HTTP GET {} failed: {} {}\nResponse: {}",
            uri.toString(),
            response.getStatus(),
            response.getReason(),
            body);
    }

    return body;
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

    pid_t pid = 0;
    const char * argv[] = {command.c_str(), url.c_str(), nullptr};
    int status = posix_spawnp(&pid, command.c_str(), nullptr, nullptr, const_cast<char * const *>(argv), nullptr);

    if (status == 0)
    {
        int wait_status = 0;
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
    catch (const std::exception &)
    {
        return 0;
    }
}

std::unique_ptr<JWTProvider> createJwtProvider(
    JWTProviderOptions options,
    const std::string & host,
    std::ostream & out,
    std::ostream & err)
{
    if (isCloudEndpoint(host))
        return std::make_unique<CloudJWTProvider>(std::move(options), host, out, err);

    return std::make_unique<JWTProvider>(std::move(options), out, err);
}

}

#endif
