#include <Server/ArrowFlight/AuthMiddleware.h>
#include <Core/UUID.h>

#if USE_ARROWFLIGHT

#include <Server/ArrowFlight/CallsData.h>

#include <Core/ServerSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>

#include <Poco/Exception.h>
#include <Poco/String.h>
#include <Poco/URI.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SESSION_TIMEOUT;
}

namespace ServerSetting
{
    extern const ServerSettingsString default_session_user;
}

void AuthMiddleware::sendingHeaders(grpc::ServerContext & context) const
{
    if (!token.empty())
        context.AddInitialMetadata(AUTHORIZATION_HEADER, "Bearer " + token);
}

void AuthMiddleware::callCompleted()
{
    if (!session)
        return;

    if (!session_id.empty())
    {
        if (session_close)
        {
            calls_data.closeSessionPreparedStatements(session_id, username);
            session->closeSession(session_id);
        }
        else
        {
            if (calls_data.usesSessionTimeoutForPsLifetime() && session_timeout.count() > 0)
                calls_data.refreshSessionPreparedStatements(session_id, username, std::chrono::duration_cast<ArrowFlight::Duration>(session_timeout));
            else if (auto ps_lifetime = calls_data.getPreparedStatementsLifetime())
                calls_data.refreshSessionPreparedStatements(session_id, username, *ps_lifetime);
            session->releaseSessionID();
        }
    }
    else if (auto ps_lifetime = calls_data.getPreparedStatementsLifetime())
    {
        calls_data.refreshSessionPreparedStatements("", username, *ps_lifetime);
    }
}


namespace
{
    std::chrono::steady_clock::duration parseSessionTimeout(
        const Poco::Util::AbstractConfiguration & config,
        unsigned query_session_timeout)
    {
        unsigned session_timeout = config.getInt("default_session_timeout", 60);

        if (query_session_timeout)
        {
            session_timeout = query_session_timeout;
            unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);

            if (session_timeout > max_session_timeout)
                throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Session timeout '{}' is larger than max_session_timeout: {}. "
                    "Maximum session timeout could be modified in configuration file.",
                    session_timeout, max_session_timeout);
        }

        return std::chrono::seconds(session_timeout);
    }

    using CallHeaders = std::multimap<grpc::string_ref, grpc::string_ref>; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- the type of gRPC

    std::optional<std::string_view> findHeader(const CallHeaders & headers, std::string_view name)
    {
        auto it = std::ranges::find_if(
            headers, [&](const auto & p) { return Poco::toLower(std::string(p.first.data(), p.first.size())) == name; });
        if (it == headers.end())
            return std::nullopt;
        return std::string_view(it->second.data(), it->second.size());
    }

    std::optional<std::pair<std::string, std::string>> getCredentialsFromBasicHeader(const CallHeaders & headers)
    {
        auto header = findHeader(headers, "authorization");
        if (!header)
            return std::nullopt;

        const std::string basic_prefix = "basic ";
        const auto auth_str = *header;

        if (!Poco::toLower(std::string(auth_str)).starts_with(basic_prefix))
            return std::nullopt;

        /// Some clients (e.g. the Go Flight client used by the ADBC Flight SQL driver) send the Base64-encoded
        /// credentials without the '=' padding. The `BASE64_NO_PADDING` option accepts both padded and unpadded input.
        std::string credentials;
        try
        {
            credentials = base64Decode(std::string(auth_str.substr(basic_prefix.size())), /* url_encoding = */ false, /* no_padding = */ true);
        }
        catch (const Poco::Exception &)
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Cannot decode the Base64-encoded credentials in the 'authorization' header");
        }

        auto pos = credentials.find(':');
        if (pos == std::string::npos)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Malformed credentials in the 'authorization' header");

        return {{credentials.substr(0, pos), credentials.substr(pos+1)}};
    }

    std::optional<std::string> getTokenFromBearerHeader(const CallHeaders & headers)
    {
        auto header = findHeader(headers, "authorization");
        if (!header)
            return std::nullopt;

        const std::string bearer_prefix = "bearer ";
        const auto auth_str = *header;

        if (!Poco::toLower(std::string(auth_str)).starts_with(bearer_prefix))
            return std::nullopt;

        return std::string(auth_str.substr(bearer_prefix.size()));
    }

    bool hasAuthorizationHeader(const CallHeaders & headers)
    {
        return findHeader(headers, "authorization").has_value();
    }

    /// Extracts the client's address from the call context.
    Poco::Net::SocketAddress getClientAddress(const grpc::ServerContext & context)
    {
        /// Returns a string like ipv4:127.0.0.1:55930 or ipv6:%5B::1%5D:55930
        String uri_encoded_peer = context.peer();

        constexpr const std::string_view ipv4_prefix = "ipv4:";
        constexpr const std::string_view ipv6_prefix = "ipv6:";

        bool ipv4 = uri_encoded_peer.starts_with(ipv4_prefix);
        bool ipv6 = uri_encoded_peer.starts_with(ipv6_prefix);

        if (!ipv4 && !ipv6)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ipv4 or ipv6 protocol in peer address, got {}", uri_encoded_peer);

        auto prefix = ipv4 ? ipv4_prefix : ipv6_prefix;
        auto family = ipv4 ? Poco::Net::AddressFamily::Family::IPv4 : Poco::Net::AddressFamily::Family::IPv6;

        uri_encoded_peer = uri_encoded_peer.substr(prefix.length());

        String peer;
        Poco::URI::decode(uri_encoded_peer, peer);

        return Poco::Net::SocketAddress{family, peer};
    }
}


String AuthMiddlewareFactory::TokenStorage::getToken(std::string username, std::string password)
{
    std::lock_guard<std::mutex> lock(token_mutex);

    cleanupExpiredTokens();

    auto token = toString(UUIDHelpers::generateV4());
    auto expiration_time = std::chrono::steady_clock::now() + std::chrono::seconds(config.getInt("default_session_timeout", 60));
    auto exp_iter = token_expiration_list.insert({expiration_time, SecretString(token)});
    token_expiration_list_index[SecretString(token)] = exp_iter;
    token_to_credentials[SecretString(token)] = {username, SecretString(password)};

    return token;
}

std::optional<std::pair<std::string, std::string>> AuthMiddlewareFactory::TokenStorage::getCredentials(std::string token)
{
    std::lock_guard<std::mutex> lock(token_mutex);

    cleanupExpiredTokens();

    auto it = token_to_credentials.find(token);
    if (it != token_to_credentials.end())
    {
        auto expiration_time = std::chrono::steady_clock::now() + std::chrono::seconds(config.getInt("default_session_timeout", 60));
        auto new_iter = token_expiration_list.insert({expiration_time, it->first});
        token_expiration_list.erase(token_expiration_list_index[it->first]);
        token_expiration_list_index[it->first] = new_iter;
        return std::pair{it->second.first, String(it->second.second)};
    }
    return std::nullopt;
}

void AuthMiddlewareFactory::TokenStorage::cleanupExpiredTokens()
{
    auto now = std::chrono::steady_clock::now();
    for (auto it = token_expiration_list.begin(); it != token_expiration_list.end() && it->first <= now;)
    {
        token_to_credentials.erase(it->second);
        token_expiration_list_index.erase(it->second);
        it = token_expiration_list.erase(it);
    }
}

ArrowFlight::Status AuthMiddlewareFactory::startCall(grpc::ServerContext & context, std::unique_ptr<AuthMiddleware> & middleware)
{
    using ArrowFlight::Status;
    const auto & headers = context.client_metadata();

    std::string username;
    std::string password;
    std::string token;
    auto session = std::make_shared<Session>(server.context(), ClientInfo::Interface::ARROW_FLIGHT);

    bool auth = false;

    try
    {
        if (auto credentials = getCredentialsFromBasicHeader(headers))
        {
            auth = true;
            std::tie(username, password) = *credentials;
        }
        else if (auto token_opt = getTokenFromBearerHeader(headers); token_opt)
        {
            if (*token_opt != "None")
            {
                token = *token_opt;
                credentials = token_storage.getCredentials(token);
                if (!credentials)
                    return Status::FlightError(Status::FlightCode::Unauthenticated, "Session expired or not authenticated.");

                std::tie(username, password) = *credentials;
            }
        }
        else if (hasAuthorizationHeader(headers))
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Unsupported 'authorization' header");
        }

        /// An empty user name (no `authorization` header, or Basic credentials with an empty
        /// user name) means the default session user (the `default_session_user` server setting).
        /// A Bearer token cannot carry an empty user name: tokens are issued below after
        /// authentication, when the user name has already been resolved.
        if (username.empty())
        {
            username = server.context()->getServerSettings()[ServerSetting::default_session_user];

            /// The default session user can be explicitly configured to be empty to prohibit
            /// connections without a user name. The reject is recorded in `system.session_log`
            /// as a login failure, so that prohibited anonymous attempts remain auditable.
            if (username.empty())
            {
                auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED,
                    "Anonymous connections are prohibited (the `default_session_user` server setting is empty), specify a user name.");
                session->onAuthenticationFailure(username, getClientAddress(context), exception);
                return Status::FlightError(Status::FlightCode::Unauthenticated, exception.message());
            }
        }

        session->authenticate(username, password, getClientAddress(context));
    }
    catch (DB::Exception & e)
    {
        return Status::FlightError(Status::FlightCode::Unauthenticated, e.what());
    }
    catch (...)
    {
        /// An exception escaping this method would be caught inside gRPC and converted to a meaningless
        /// "Unexpected error in RPC handling" status, so we convert it to a proper error status ourselves.
        return Status::FlightError(Status::FlightCode::Unauthenticated, getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }

    try
    {
        auto header_or_empty = [&](std::string_view name) { return std::string(findHeader(headers, name).value_or("")); };
        std::string session_id = header_or_empty("x-clickhouse-session-id");
        std::string session_check = header_or_empty("x-clickhouse-session-check");
        std::string session_timeout_str = header_or_empty("x-clickhouse-session-timeout");

        unsigned session_timeout = 0;
        if (!session_timeout_str.empty())
        {
            ReadBufferFromString buf(session_timeout_str);
            if (!tryReadIntText(session_timeout, buf) || !buf.eof())
                return Status::Invalid("Invalid session timeout: " + session_timeout_str);
        }

        std::string session_close = header_or_empty("x-clickhouse-session-close");

        if (session_id.empty())
            session->makeSessionContext();
        else
            session->makeSessionContext(session_id, parseSessionTimeout(server.context()->getConfigRef(), session_timeout), session_check == "1");

        if (auth)
            token = token_storage.getToken(username, password);

        auto parsed_session_timeout = session_id.empty()
            ? std::chrono::steady_clock::duration{0}
            : parseSessionTimeout(server.context()->getConfigRef(), session_timeout);

        middleware = std::make_unique<AuthMiddleware>(session, token, username, calls_data, session_id, session_close == "1" && server.config().getBool("enable_arrow_close_session", true), parsed_session_timeout);
    }
    catch (DB::Exception & e)
    {
        return Status::Invalid(e.what());
    }
    catch (...)
    {
        return Status::Invalid(getCurrentExceptionMessage(/* with_stacktrace = */ false));
    }

    middleware->sendingHeaders(context);
    return Status::OK();
}

}

#endif
