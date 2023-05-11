#include "HTTPWebSocketHandler.h"
#include "IServer.h"

#include <Server/HTTPHandlerFactory.h>

#include <Access/Authentication.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>

#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/NetException.h"
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>
#include <Common/logger_useful.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Parsers/ASTSetQuery.h>
#include <Poco/Base64Decoder.h>



#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConcatReadBuffer.h>
#include <Core/ExternalTable.h>
#include <Compression/CompressedReadBuffer.h>

using Poco::Net::WebSocketException;
using Poco::Util::Application;


#if USE_SSL
#include <Poco/Net/X509Certificate.h>
#endif

namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_COMPILE_REGEXP;

    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int UNEXPECTED_AST_STRUCTURE;

    extern const int SYNTAX_ERROR;

    extern const int INCORRECT_DATA;
    extern const int TYPE_MISMATCH;

    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE;
    extern const int UNKNOWN_STORAGE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_SETTING;
    extern const int UNKNOWN_DIRECTION_OF_SORTING;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;

    extern const int QUERY_IS_TOO_LARGE;

    extern const int NOT_IMPLEMENTED;
    extern const int SOCKET_TIMEOUT;

    extern const int UNKNOWN_USER;
    extern const int WRONG_PASSWORD;
    extern const int REQUIRED_PASSWORD;
    extern const int AUTHENTICATION_FAILED;

    extern const int INVALID_SESSION_TIMEOUT;
    extern const int HTTP_LENGTH_REQUIRED;
    extern const int SUPPORT_IS_DISABLED;

    extern const int TIMEOUT_EXCEEDED;
}


using Poco::IOException;

//static std::chrono::steady_clock::duration parseSessionTimeout(
//    const Poco::Util::AbstractConfiguration & config,
//    const HTMLForm & params)
//{
//    unsigned session_timeout = config.getInt("default_session_timeout", 60);
//
//    if (params.has("session_timeout"))
//    {
//        unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);
//        std::string session_timeout_str = params.get("session_timeout");
//
//        ReadBufferFromString buf(session_timeout_str);
//        if (!tryReadIntText(session_timeout, buf) || !buf.eof())
//            throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Invalid session timeout: '{}'", session_timeout_str);
//
//        if (session_timeout > max_session_timeout)
//            throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Session timeout '{}' is larger than max_session_timeout: {}. "
//                                                                 "Maximum session timeout could be modified in configuration file.",
//                            session_timeout_str, max_session_timeout);
//    }
//
//    return std::chrono::seconds(session_timeout);
//}

static String base64Encode(const String & decoded)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    Poco::Base64Encoder encoder(ostr);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

static String base64Decode(const String & encoded)
{
    String decoded;
    Poco::MemoryInputStream istr(encoded.data(), encoded.size());
    Poco::Base64Decoder decoder(istr);
    Poco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

HTTPWebSocketHandler::HTTPWebSocketHandler(IServer & server_)
    : server(server_)
    , default_settings(server.context()->getSettingsRef())
{
}

bool HTTPWebSocketHandler::authenticateUser(
    HTTPServerRequest & request,
    HTMLForm & params,
    HTTPServerResponse & response)
{
    using namespace Poco::Net;

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("X-ClickHouse-User", "");
    std::string password = request.get("X-ClickHouse-Key", "");
    std::string quota_key = request.get("X-ClickHouse-Quota", "");

    /// The header 'X-ClickHouse-SSL-Certificate-Auth: on' enables checking the common name
    /// extracted from the SSL certificate used for this connection instead of checking password.
    bool has_ssl_certificate_auth = (request.get("X-ClickHouse-SSL-Certificate-Auth", "") == "on");
    bool has_auth_headers = !user.empty() || !password.empty() || !quota_key.empty() || has_ssl_certificate_auth;

    /// User name and password can be passed using HTTP Basic auth or query parameters
    /// (both methods are insecure).
    bool has_http_credentials = request.hasCredentials();
    bool has_credentials_in_query_params = params.has("user") || params.has("password") || params.has("quota_key");

    std::string spnego_challenge;
    std::string certificate_common_name;

    if (has_auth_headers)
    {
        /// It is prohibited to mix different authorization schemes.
        if (has_http_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Invalid authentication: it is not allowed "
                            "to use SSL certificate authentication and Authorization HTTP header simultaneously");
        if (has_credentials_in_query_params)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Invalid authentication: it is not allowed "
                            "to use SSL certificate authentication and authentication via parameters simultaneously simultaneously");

        if (has_ssl_certificate_auth)
        {
#if USE_SSL
            if (!password.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                                "Invalid authentication: it is not allowed "
                                "to use SSL certificate authentication and authentication via password simultaneously");

            if (request.havePeerCertificate())
                certificate_common_name = request.peerCertificate().commonName();

            if (certificate_common_name.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                                "Invalid authentication: SSL certificate authentication requires nonempty certificate's Common Name");
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "SSL certificate authentication disabled because ClickHouse was built without SSL library");
#endif
        }
    }
    else if (has_http_credentials)
    {
        /// It is prohibited to mix different authorization schemes.
        if (has_credentials_in_query_params)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Invalid authentication: it is not allowed "
                            "to use Authorization HTTP header and authentication via parameters simultaneously");

        std::string scheme;
        std::string auth_info;
        request.getCredentials(scheme, auth_info);

        if (Poco::icompare(scheme, "Basic") == 0)
        {
            HTTPBasicCredentials credentials(auth_info);
            user = credentials.getUsername();
            password = credentials.getPassword();
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

        quota_key = params.get("quota_key", "");
    }
    else
    {
        /// If the user name is not set we assume it's the 'default' user.
        user = params.get("user", "default");
        password = params.get("password", "");
        quota_key = params.get("quota_key", "");
    }

    if (!certificate_common_name.empty())
    {
        if (!request_credentials)
            request_credentials = std::make_unique<SSLCertificateCredentials>(user, certificate_common_name);

        auto * certificate_credentials = dynamic_cast<SSLCertificateCredentials *>(request_credentials.get());
        if (!certificate_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: expected SSL certificate authorization scheme");
    }
    else if (!spnego_challenge.empty())
    {
        if (!request_credentials)
            request_credentials = server.context()->makeGSSAcceptorContext();

        auto * gss_acceptor_context = dynamic_cast<GSSAcceptorContext *>(request_credentials.get());
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
            return false;
        }
    }
    else // I.e., now using user name and password strings ("Basic").
    {
        if (!request_credentials)
            request_credentials = std::make_unique<BasicCredentials>();

        auto * basic_credentials = dynamic_cast<BasicCredentials *>(request_credentials.get());
        if (!basic_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: expected 'Basic' HTTP Authorization scheme");

        basic_credentials->setUserName(user);
        basic_credentials->setPassword(password);
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.
    ClientInfo & client_info = session->getClientInfo();

    ClientInfo::HTTPMethod http_method = ClientInfo::HTTPMethod::UNKNOWN;
    if (request.getMethod() == HTTPServerRequest::HTTP_GET)
        http_method = ClientInfo::HTTPMethod::GET;
    else if (request.getMethod() == HTTPServerRequest::HTTP_POST)
        http_method = ClientInfo::HTTPMethod::POST;

    client_info.http_method = http_method;
    client_info.http_user_agent = request.get("User-Agent", "");
    client_info.http_referer = request.get("Referer", "");
    client_info.forwarded_for = request.get("X-Forwarded-For", "");
    client_info.quota_key = quota_key;

    /// Extract the last entry from comma separated list of forwarded_for addresses.
    /// Only the last proxy can be trusted (if any).
    String forwarded_address = client_info.getLastForwardedFor();
    try
    {
        if (!forwarded_address.empty() && server.config().getBool("auth_use_forwarded_address", false))
            session->authenticate(*request_credentials, Poco::Net::SocketAddress(forwarded_address, request.clientAddress().port()));
        else
            session->authenticate(*request_credentials, request.clientAddress());
    }
    catch (const Authentication::Require<BasicCredentials> & required_credentials)
    {
        request_credentials = std::make_unique<BasicCredentials>();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Basic");
        else
            response.set("WWW-Authenticate", "Basic realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        return false;
    }
    catch (const Authentication::Require<GSSAcceptorContext> & required_credentials)
    {
        request_credentials = server.context()->makeGSSAcceptorContext();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Negotiate");
        else
            response.set("WWW-Authenticate", "Negotiate realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        return false;
    }

    request_credentials.reset();
    return true;
}


void HTTPWebSocketHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    setThreadName("WebSocket");
    ThreadStatus thread_status;

    session = std::make_shared<Session>(server.context(), ClientInfo::Interface::WEB_SOCKET, request.isSecure());
    SCOPE_EXIT({ session.reset(); });
    std::optional<CurrentThread::QueryScope> query_scope;

    String session_id;

    Application& app = Application::instance();
    try
    {

        HTMLForm params(default_settings, request);

        app.logger().information("Request URI: %s", request.getURI());

        if (!authenticateUser(request, params, response))
            return; // '401 Unauthorized' response with 'Negotiate' has been sent at this point.

        // TODO: maybe we should not allocate it on stack or everything will go down
        WebSocket ws(request, response);
        auto connection = WebSocketServerConnection(server, ws, session);
        connection.run();
    }
    catch (WebSocketException& exc)
    {
        app.logger().log(exc);
        switch (exc.code())
        {
            case WebSocket::WS_ERR_HANDSHAKE_UNSUPPORTED_VERSION:
                response.set("Sec-WebSocket-Version", WebSocket::WEBSOCKET_VERSION);
                break;
            case WebSocket::WS_ERR_NO_HANDSHAKE:
            case WebSocket::WS_ERR_HANDSHAKE_NO_VERSION:
            case WebSocket::WS_ERR_HANDSHAKE_NO_KEY:
                response.setStatusAndReason(HTTPResponse::HTTP_BAD_REQUEST);
                response.setContentLength(0);
                response.send();
                break;
        }
    }
}

HTTPRequestHandlerFactoryPtr
createHTTPWebSocketMainHandlerFactory(IServer & server, const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);

    auto main_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<DynamicQueryWebSocketHandler>>(server,"query");
    main_handler->attachNonStrictPath("/");
    main_handler->allowGetAndHeadRequest();
    factory->addHandler(main_handler);

    return factory;
}

DynamicQueryWebSocketHandler::DynamicQueryWebSocketHandler(IServer & server_, const std::string & param_name_)
    : HTTPWebSocketHandler(server_),
    param_name(param_name_)
{
}


bool DynamicQueryWebSocketHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (key == param_name)
        return true;    /// do nothing

    if (startsWith(key, QUERY_PARAMETER_NAME_PREFIX))
    {
        /// Save name and values of substitution in dictionary.
        const String parameter_name = key.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));

        if (!context->getQueryParameters().contains(parameter_name))
            context->setQueryParameter(parameter_name, value);
        return true;
    }

    return false;
}

    std::string DynamicQueryWebSocketHandler::getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context)
{
    if (likely(!startsWith(request.getContentType(), "multipart/form-data")))
    {
        /// Part of the query can be passed in the 'query' parameter and the rest in the request body
        /// (http method need not necessarily be POST). In this case the entire query consists of the
        /// contents of the 'query' parameter, a line break and the request body.
        std::string query_param = params.get(param_name, "");
        return query_param.empty() ? query_param : query_param + "\n";
    }

    /// Support for "external data for query processing".
    /// Used in case of POST request with form-data, but it isn't expected to be deleted after that scope.
    ExternalTablesHandler handler(context, params);
    params.load(request, request.getStream(), handler);

    std::string full_query;
    /// Params are of both form params POST and uri (GET params)
    for (const auto & it : params)
    {
        if (it.first == param_name)
        {
            full_query += it.second;
        }
        else
        {
            customizeQueryParam(context, it.first, it.second);
        }
    }

    return full_query;
}


}
