#pragma once

#include "config.h"

#if USE_ARROWFLIGHT

#include <Interpreters/Session.h>
#include <Server/IServer.h>
#include <Server/ArrowFlight/FlightStatus.h>
#include <Common/SecretString.h>
#include <Common/StringHashForHeterogeneousLookup.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <grpcpp/server_context.h>

#include <functional>
#include <map>
#include <unordered_map>

namespace DB::ArrowFlight
{
class CallsData;
}

namespace DB
{

inline const std::string AUTHORIZATION_HEADER = "authorization";

/// The authentication of one Arrow Flight call: made by `AuthMiddlewareFactory::startCall` before the call is
/// handled, from its `authorization` and `x-clickhouse-session-*` headers. It holds the session the call runs in.
class AuthMiddleware
{
public:
    explicit AuthMiddleware(std::shared_ptr<Session> session_, const std::string & token_, const std::string & username_,
                            ArrowFlight::CallsData & calls_data_,
                            const std::string & session_id_ = "", bool session_close_ = false,
                            std::chrono::steady_clock::duration session_timeout_ = std::chrono::steady_clock::duration{0})
        : session(session_)
        , token(token_)
        , username(username_)
        , calls_data(calls_data_)
        , session_id(session_id_)
        , session_close(session_close_)
        , session_timeout(session_timeout_)
    {
    }

    [[nodiscard]] const std::string & getUsername() const { return username; }
    [[nodiscard]] const std::shared_ptr<Session> & getSession() const { return session; }
    [[nodiscard]] const std::string & getSessionId() const { return session_id; }
    [[nodiscard]] std::chrono::steady_clock::duration getSessionTimeout() const { return session_timeout; }

    /// Adds the headers of the response: the token a client authenticated with a password uses afterwards.
    void sendingHeaders(grpc::ServerContext & context) const;

    /// Called once the call is handled, whatever its outcome.
    void callCompleted();

    /// Returns false once the client has cancelled the call or has disconnected. It is given to the context of a query
    /// the call runs (`Context::setConnectionAliveCheck`), so that the query notices it while it waits for admission
    /// in `ProcessList`. The callback refers to the `grpc::ServerContext` of the call and must not outlive the call.
    void setConnectionAliveCheck(std::function<bool()> connection_alive_check_) { connection_alive_check = std::move(connection_alive_check_); }
    [[nodiscard]] std::function<bool()> getConnectionAliveCheck() const { return connection_alive_check; }

private:
    std::shared_ptr<Session> session;
    std::string token;
    std::string username;
    ArrowFlight::CallsData & calls_data;
    const std::string session_id;
    const bool session_close;
    const std::chrono::steady_clock::duration session_timeout;
    std::function<bool()> connection_alive_check;
};

class AuthMiddlewareFactory
{
    /// TokenStorage keeps track of issued tokens, check for expiration and expires them on any access,
    /// updates expiration time of not expired token on request for credentials (getCredentials)
    class TokenStorage
    {
    public:
        explicit TokenStorage(const Poco::Util::AbstractConfiguration & config_) : config(config_) {}

        /// Generates unique token for given credentials and saves it in storage.
        String getToken(std::string username, std::string password);

        /// Returns credential associated with specific token and updates expiration time for this token.
        /// If the token isn't found (never existed or expired) - returns empty optional.
        std::optional<std::pair<std::string, std::string>> getCredentials(std::string token);

    private:
        void cleanupExpiredTokens() TSA_REQUIRES(token_mutex);

        using TokenExpirationList = std::multimap<std::chrono::steady_clock::time_point, SecretString>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

        std::mutex token_mutex;
        TokenExpirationList token_expiration_list TSA_GUARDED_BY(token_mutex);
        std::unordered_map<SecretString, TokenExpirationList::iterator, StringHashForHeterogeneousLookup, std::equal_to<>> token_expiration_list_index TSA_GUARDED_BY(token_mutex); // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::unordered_map<SecretString, std::pair<std::string, SecretString>, StringHashForHeterogeneousLookup, std::equal_to<>> token_to_credentials TSA_GUARDED_BY(token_mutex); // STYLE_CHECK_ALLOW_STD_CONTAINERS

        const Poco::Util::AbstractConfiguration & config;
    };

public:
    explicit AuthMiddlewareFactory(IServer & server_, ArrowFlight::CallsData & calls_data_)
        : server(server_)
        , token_storage(server_.config())
        , calls_data(calls_data_)
    {}

    /// Authenticates a call. On success, `middleware` is set and has sent its headers.
    ArrowFlight::Status startCall(grpc::ServerContext & context, std::unique_ptr<AuthMiddleware> & middleware);

private:
    IServer & server;
    TokenStorage token_storage;
    ArrowFlight::CallsData & calls_data;
};

}

#endif
