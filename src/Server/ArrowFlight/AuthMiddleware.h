#pragma once

#include <Interpreters/Session.h>
#include <Server/IServer.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <arrow/flight/server_middleware.h>
#include <arrow/flight/server.h>

namespace DB::ArrowFlight
{
class CallsData;
}

namespace DB
{

inline const std::string AUTHORIZATION_HEADER = "authorization";
inline const std::string AUTHORIZATION_MIDDLEWARE_NAME = "authorization_middleware";

class AuthMiddleware : public arrow::flight::ServerMiddleware
{
public:
    explicit AuthMiddleware(std::shared_ptr<Session> session_, const std::string & token_, const std::string & username_,
                            ArrowFlight::CallsData & calls_data_,
                            const std::string & session_id_ = "", bool session_close_ = false)
        : session(session_)
        , token(token_)
        , username(username_)
        , calls_data(calls_data_)
        , session_id(session_id_)
        , session_close(session_close_)
    {
    }

    static AuthMiddleware & get(const arrow::flight::ServerCallContext & context)
    {
        return *static_cast<AuthMiddleware *>(context.GetMiddleware(AUTHORIZATION_MIDDLEWARE_NAME));
    }

    const std::string & getUsername() const { return username; }
    const std::shared_ptr<Session> & getSession() const { return session; }
    const std::string & getSessionId() const { return session_id; }

    void SendingHeaders(arrow::flight::AddCallHeaders * outgoing_headers) override;
    void CallCompleted(const arrow::Status & /*status*/) override;

    std::string name() const override { return AUTHORIZATION_MIDDLEWARE_NAME; }

private:
    std::shared_ptr<Session> session;
    std::string token;
    std::string username;
    ArrowFlight::CallsData & calls_data;
    const std::string session_id;
    const bool session_close;
};

class AuthMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory
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

        using TokenExpirationList = std::multimap<std::chrono::steady_clock::time_point, std::string>;

        std::mutex token_mutex;
        TokenExpirationList token_expiration_list TSA_GUARDED_BY(token_mutex);
        std::unordered_map<std::string, TokenExpirationList::iterator> token_expiration_list_index TSA_GUARDED_BY(token_mutex);
        std::unordered_map<std::string, std::pair<std::string, std::string>> token_to_credentials TSA_GUARDED_BY(token_mutex);

        const Poco::Util::AbstractConfiguration & config;
    };

public:
    explicit AuthMiddlewareFactory(IServer & server_, ArrowFlight::CallsData & calls_data_)
        : server(server_)
        , token_storage(server_.config())
        , calls_data(calls_data_)
    {}

    arrow::Status StartCall(
        const arrow::flight::CallInfo & /*info*/,
        const arrow::flight::ServerCallContext & context,
        std::shared_ptr<arrow::flight::ServerMiddleware> * middleware) override;

    private:
        IServer & server;
        TokenStorage token_storage;
        ArrowFlight::CallsData & calls_data;
};

}
