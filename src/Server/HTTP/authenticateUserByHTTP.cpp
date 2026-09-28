#include <Access/Authentication.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Common/Base64.h>
#include <Common/HTTPHeaderFilter.h>
#include <Server/HTTPHandler.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>

#include <Poco/Net/HTTPBasicCredentials.h>

#include <optional>

#if USE_SSL
#    include <Common/Crypto/X509Certificate.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}


namespace
{
    /// Throws an exception that multiple authorization schemes are used simultaneously.
    [[noreturn]] void throwMultipleAuthenticationMethods(std::string_view method1, std::string_view method2)
    {
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "Invalid authentication: it is not allowed to use {} and {} simultaneously", method1, method2);
    }

    /// Checks that a specified user name is not empty, and throws an exception if it's empty.
    void checkUserNameNotEmptyAndServerHasEnoughMemory(const String & user_name, std::string_view method, const ContextPtr & context)
    {
        auto users_to_ignore_early_memory_limit_check = context->getUsersToIgnoreEarlyMemoryLimitCheck();
        if (!(users_to_ignore_early_memory_limit_check && users_to_ignore_early_memory_limit_check->contains(user_name)))
        {
            LOG_TEST(getLogger("authenticateUserByHTTP"), "Checking memory limit for user: {}", user_name);
            CurrentMemoryTracker::check();
        }
        else
            LOG_TEST(getLogger("authenticateUserByHTTP"), "Skipping memory limit check for user: {}", user_name);

        if (user_name.empty())
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Got an empty user name from {}", method);
    }
}


bool authenticateUserByHTTP(
    const HTTPServerRequest & request,
    const HTMLForm & params,
    HTTPServerResponse & response,
    Session & session,
    std::unique_ptr<Credentials> & request_credentials,
    const HTTPHandlerConnectionConfig & connection_config,
    ContextPtr global_context,
    LoggerPtr log);
bool authenticateUserByHTTP(
    const HTTPServerRequest & request,
    const HTMLForm & params,
    HTTPServerResponse & response,
    Session & session,
    std::unique_ptr<Credentials> & request_credentials,
    const HTTPHandlerConnectionConfig & connection_config,
    ContextPtr global_context,
    LoggerPtr log)
{
    /// Get the credentials created by the previous call of authenticateUserByHTTP() while handling the previous HTTP request.
    auto current_credentials = std::move(request_credentials);
    const auto & config_credentials = connection_config.credentials;

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("X-ClickHouse-User", "");
    std::string password = request.get("X-ClickHouse-Key", "");
    std::string quota_key = request.get("X-ClickHouse-Quota", "");
    bool has_auth_headers = !user.empty() || !password.empty();

    /// The header 'X-ClickHouse-SSL-Certificate-Auth: on' enables checking the common name
    /// extracted from the SSL certificate used for this connection instead of checking password.
    bool has_ssl_certificate_auth = (request.get("X-ClickHouse-SSL-Certificate-Auth", "") == "on");
    bool has_config_credentials = config_credentials.has_value();

    /// User name and password can be passed using HTTP Basic auth or query parameters
    /// (both methods are insecure).
    bool has_credentials_in_query_params = params.has("user") || params.has("password");

    /// Whether the request carries an `Authorization` header that should be treated as
    /// credentials. The sentinel value `never` (which `play.html` sets on the requests it can
    /// add headers to) disables it.
    bool has_authorization_header = request.hasCredentials() && request.get("Authorization") != "never";

    /// Credentials passed in the URL query parameters take precedence over the HTTP
    /// `Authorization` header: when both are present, the header is ignored instead of
    /// rejecting the request for mixing authentication methods.
    ///
    /// This is needed because once a browser has remembered HTTP Basic credentials for an
    /// origin, it attaches the `Authorization` header to every subsequent request to that
    /// origin automatically - including requests that the application has no way to add or
    /// remove headers from, such as a form submission or a download navigation. The Web UI
    /// (`play.html`) authenticates by putting the user name and password into the URL query
    /// parameters, so without this precedence such a request would carry both the remembered
    /// header and the parameters and be rejected. (The special value `Authorization: never`
    /// also suppresses the header, but it can only be set from a scripted request such as
    /// `fetch` or `XHR`, not from a plain navigation.)
    ///
    /// This precedence applies only to the default authentication path. When the handler has
    /// its own configured credentials, an `Authorization` header is still rejected as a mix of
    /// authentication methods, regardless of the query parameters (see below).
    bool has_http_credentials = has_authorization_header && !has_credentials_in_query_params;

    std::string spnego_challenge;
#if USE_SSL
    X509Certificate::Subjects certificate_subjects;

    /// Capture the TLS client certificate (if the client presented one) regardless of the selected
    /// authentication method, so that session_log records it even when the connection authenticates
    /// by another method (headers, basic, query parameters, config) or the login fails.
    /// Mirrors the native protocol path in TCPHandler::receiveHello.
    std::optional<X509Certificate> peer_certificate;
    if (request.havePeerCertificate())
    {
        peer_certificate = request.peerCertificate();
        session.setClientCertificate(*peer_certificate);
    }
#endif

    if (config_credentials)
    {
        checkUserNameNotEmptyAndServerHasEnoughMemory(config_credentials->getUserName(), "config authentication", global_context);
    }
    if (has_ssl_certificate_auth)
    {
#if USE_SSL
        /// For SSL certificate authentication we extract the user name from the "X-ClickHouse-User" HTTP header.
        checkUserNameNotEmptyAndServerHasEnoughMemory(user, "X-ClickHouse HTTP headers", global_context);

        /// It is prohibited to mix different authorization schemes.
        if (has_config_credentials)
            throwMultipleAuthenticationMethods("SSL certificate authentication", "authentication set in config");
        if (!password.empty())
            throwMultipleAuthenticationMethods("SSL certificate authentication", "authentication via password");
        if (has_http_credentials)
            throwMultipleAuthenticationMethods("SSL certificate authentication", "Authorization HTTP header");
        if (has_credentials_in_query_params)
            throwMultipleAuthenticationMethods("SSL certificate authentication", "authentication via parameters");

        if (peer_certificate)
            certificate_subjects = peer_certificate->extractAllSubjects();

        if (certificate_subjects.empty())
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Invalid authentication: SSL certificate authentication requires nonempty certificate's Common Name or Subject Alternative Name");
#else
        UNUSED(log);
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "SSL certificate authentication disabled because ClickHouse was built without SSL library");
#endif
    }
    else if (has_auth_headers)
    {
        checkUserNameNotEmptyAndServerHasEnoughMemory(user, "X-ClickHouse HTTP headers", global_context);

        /// It is prohibited to mix different authorization schemes.
        if (has_config_credentials)
            throwMultipleAuthenticationMethods("X-ClickHouse HTTP headers", "authentication set in config");
        if (has_http_credentials)
            throwMultipleAuthenticationMethods("X-ClickHouse HTTP headers", "Authorization HTTP header");
        if (has_credentials_in_query_params)
            throwMultipleAuthenticationMethods("X-ClickHouse HTTP headers", "authentication via parameters");
    }
    else if (has_http_credentials)
    {
        /// It is prohibited to mix different authorization schemes.
        /// (Authentication via query parameters takes precedence over the `Authorization`
        /// header and is handled above by excluding it from `has_http_credentials`.)
        if (has_config_credentials)
            throwMultipleAuthenticationMethods("Authorization HTTP header", "authentication set in config");

        std::string scheme;
        std::string auth_info;
        request.getCredentials(scheme, auth_info);

        if (Poco::icompare(scheme, "Basic") == 0)
        {
            Poco::Net::HTTPBasicCredentials credentials(auth_info);
            user = credentials.getUsername();
            password = credentials.getPassword();
            checkUserNameNotEmptyAndServerHasEnoughMemory(user, "Authorization HTTP header", global_context);
        }
        else if (Poco::icompare(scheme, "Negotiate") == 0)
        {
            spnego_challenge = auth_info;

            if (spnego_challenge.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: SPNEGO challenge is empty");
        }
        else
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: '{}' HTTP Authorization scheme is not supported", scheme);
        }
    }
    else
    {
        /// Authentication via the URL query parameters (or, if absent, the 'default' user).
        /// The query parameters take precedence over the `Authorization` header (which was
        /// excluded from `has_http_credentials` above), but mixing the header with credentials
        /// configured for the handler is still rejected, as for every other method.
        if (has_config_credentials && has_authorization_header)
            throwMultipleAuthenticationMethods("Authorization HTTP header", "authentication set in config");

        /// If the user name is not set we assume it's the 'default' user.
        user = params.get("user", "default");
        password = params.get("password", "");
        checkUserNameNotEmptyAndServerHasEnoughMemory(user, "authentication via parameters", global_context);
    }

    if (has_config_credentials)
    {
        current_credentials = std::make_unique<AlwaysAllowCredentials>(*config_credentials);
    }
#if USE_SSL
    else if (!certificate_subjects.empty())
    {
        chassert(!user.empty());
        if (!current_credentials)
            current_credentials = std::make_unique<SSLCertificateCredentials>(user, std::move(certificate_subjects));

        auto * certificate_credentials = dynamic_cast<SSLCertificateCredentials *>(current_credentials.get());
        if (!certificate_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: expected SSL certificate authorization scheme");
    }
    else if (!spnego_challenge.empty())
    {
        if (!current_credentials)
            current_credentials = global_context->makeGSSAcceptorContext();

        auto * gss_acceptor_context = dynamic_cast<GSSAcceptorContext *>(current_credentials.get());
        if (!gss_acceptor_context)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: unexpected 'Negotiate' HTTP Authorization scheme expected");

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunreachable-code"
        const auto spnego_response = base64Encode(gss_acceptor_context->processToken(base64Decode(spnego_challenge), log));
#pragma clang diagnostic pop
        if (!spnego_response.empty())
            response.set("WWW-Authenticate", "Negotiate " + spnego_response);

        if (!gss_acceptor_context->isFailed() && !gss_acceptor_context->isReady())
        {
            if (spnego_response.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: 'Negotiate' HTTP Authorization failure");

            response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
            response.send();
            /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
            request_credentials = std::move(current_credentials);
            return false;
        }
    }
#endif
    else // I.e., now using user name and password strings ("Basic").
    {
        if (!current_credentials)
            current_credentials = std::make_unique<BasicCredentials>();

        auto * basic_credentials = dynamic_cast<BasicCredentials *>(current_credentials.get());
        if (!basic_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: expected 'Basic' HTTP Authorization scheme");

        if (request.get("Authorization", "") != "never")
            basic_credentials->enableInteractiveBasicAuthenticationInTheBrowser();

        chassert(!user.empty());
        basic_credentials->setUserName(user);
        basic_credentials->setPassword(password);
    }

    if (params.has("quota_key"))
    {
        if (!quota_key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Invalid authentication: it is not allowed "
                            "to use quota key as HTTP header and as parameter simultaneously");

        quota_key = params.get("quota_key");
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.

    session.setHTTPClientInfo(request);
    session.setQuotaClientKey(quota_key);

    /// Extract the last entry from comma separated list of forwarded_for addresses.
    /// Only the last proxy can be trusted (if any).
    auto forwarded_address = session.getClientInfo().getLastForwardedFor();
    try
    {
        if (forwarded_address && global_context->getConfigRef().getBool("auth_use_forwarded_address", false))
            session.authenticate(*current_credentials, *forwarded_address, request.clientAddress());
        else
            session.authenticate(*current_credentials, request.clientAddress());
    }
    catch (const Authentication::Require<BasicCredentials> & required_credentials)
    {
        current_credentials = std::make_unique<BasicCredentials>();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Basic");
        else
            response.set("WWW-Authenticate", "Basic realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
        request_credentials = std::move(current_credentials);
        return false;
    }
    catch (const Authentication::Require<GSSAcceptorContext> & required_credentials)
    {
        current_credentials = global_context->makeGSSAcceptorContext();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Negotiate");
        else
            response.set("WWW-Authenticate", "Negotiate realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
        request_credentials = std::move(current_credentials);
        return false;
    }

    return true;
}

}
