#include <Server/ArrowFlight/AuthMiddleware.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Base64.h>
#include <Interpreters/Context.h>

#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SESSION_TIMEOUT;
}

void AuthMiddleware::SendingHeaders(arrow::flight::AddCallHeaders * outgoing_headers)
{
    if (!token.empty())
        outgoing_headers->AddHeader(AUTHORIZATION_HEADER, "Bearer " + token);
}

void AuthMiddleware::CallCompleted(const arrow::Status & /*status*/)
{
    if (!session)
        return;

    if (!session_id.empty())
    {
        if (session_close)
            session->closeSession(session_id);
        else
            session->releaseSessionID();
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

    std::optional<std::pair<std::string, std::string>> getCredentialsFromBasicHeader(const arrow::flight::CallHeaders & headers)
    {
        auto it = std::ranges::find_if(headers, [](const auto & p) { return Poco::toLower(std::string(p.first)) == "authorization"; });
        if (it == headers.end())
            return std::nullopt;

        const std::string basic_prefix = "basic ";
        const auto & auth_str = it->second;

        if (!Poco::toLower(std::string(auth_str)).starts_with(basic_prefix))
            return std::nullopt;

        auto credentials = base64Decode(std::string(auth_str.substr(basic_prefix.size())));

        auto pos = credentials.find(':');
        if (pos == std::string::npos)
            return {{credentials, ""}};

        return {{credentials.substr(0, pos), credentials.substr(pos+1)}};
    }

    std::optional<std::string> getTokenFromBearerHeader(const arrow::flight::CallHeaders & headers)
    {
        auto it = std::ranges::find_if(headers, [](const auto & p) { return Poco::toLower(std::string(p.first)) == "authorization"; });
        if (it == headers.end())
            return std::nullopt;

        const std::string bearer_prefix = "bearer ";
        const auto & auth_str = it->second;

        if (!Poco::toLower(std::string(auth_str)).starts_with(bearer_prefix))
            return std::nullopt;

        return std::string(auth_str.substr(bearer_prefix.size()));
    }

    /// Extracts the client's address from the call context.
    Poco::Net::SocketAddress getClientAddress(const arrow::flight::ServerCallContext & context)
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
    auto exp_iter = token_expiration_list.insert({expiration_time, token});
    token_expiration_list_index[token] = exp_iter;
    token_to_credentials[token] = {username, password};

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
        auto new_iter = token_expiration_list.insert({expiration_time, token});
        token_expiration_list.erase(token_expiration_list_index[token]);
        token_expiration_list_index[token] = new_iter;
        return it->second;
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

arrow::Status AuthMiddlewareFactory::StartCall(
        const arrow::flight::CallInfo & /*info*/,
        const arrow::flight::ServerCallContext & context,
        std::shared_ptr<arrow::flight::ServerMiddleware> * middleware)
{
    const auto & headers = context.incoming_headers();

    std::string username("default");
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
        else if (auto token_opt = getTokenFromBearerHeader(headers); token_opt && *token_opt != "None")
        {
            token = *token_opt;
            credentials = token_storage.getCredentials(token);
            if (!credentials)
                return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated, "Session expired or not authenticated.");

            std::tie(username, password) = *credentials;
        }
        session->authenticate(username, password, getClientAddress(context));
    }
    catch (DB::Exception & e)
    {
        return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthenticated, e.what());
    }

    try
    {
        std::string session_id;
        auto session_it = headers.find("x-clickhouse-session-id");
        if (session_it != headers.end())
            session_id = std::string(session_it->second);

        std::string session_check;
        session_it = headers.find("x-clickhouse-session-check");
        if (session_it != headers.end())
            session_check = std::string(session_it->second);

        std::string session_timeout_str;
        session_it = headers.find("x-clickhouse-session-timeout");
        if (session_it != headers.end())
            session_timeout_str = std::string(session_it->second);

        unsigned session_timeout = 0;
        if (!session_timeout_str.empty())
        {
            ReadBufferFromString buf(session_timeout_str);
            if (!tryReadIntText(session_timeout, buf) || !buf.eof())
                return arrow::Status::Invalid("Invalid session timeout: " + session_timeout_str);
        }

        std::string session_close;
        session_it = headers.find("x-clickhouse-session-close");
        if (session_it != headers.end())
            session_close = std::string(session_it->second);

        if (session_id.empty())
            session->makeSessionContext();
        else
            session->makeSessionContext(session_id, parseSessionTimeout(server.context()->getConfigRef(), session_timeout), session_check == "1");

        if (auth)
            token = token_storage.getToken(username, password);

        *middleware = std::make_unique<AuthMiddleware>(session, token, username, session_id, session_close == "1" && server.config().getBool("enable_arrow_close_session", true));
    }
    catch (DB::Exception & e)
    {
        return arrow::Status::Invalid(e.what());
    }

    return arrow::Status::OK();
}

}
