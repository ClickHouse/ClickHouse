#pragma once

#include <Common/SettingsChanges.h>
#include <Access/AuthenticationData.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SessionTracker.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>

namespace Poco::Net { class SocketAddress; }

namespace DB
{
class Credentials;
class AuthenticationData;
struct NamedSessionData;
class NamedSessionsStorage;
struct User;
using UserPtr = std::shared_ptr<const User>;
class SessionLog;

/** Represents user-session from the server perspective,
 *  basically it is just a smaller subset of Context API, simplifies Context management.
 *
 * Holds session context, facilitates acquisition of NamedSession and proper creation of query contexts.
 */
class Session
{
public:
    /// Stops using named sessions. The method must be called at the server shutdown.
    static void shutdownNamedSessions();

    Session(const ContextPtr & global_context_, ClientInfo::Interface interface_, bool is_secure = false, const std::string & certificate = "");
    ~Session();

    Session(const Session &&) = delete;
    Session & operator=(const Session &&) = delete;
    Session(const Session &) = delete;
    Session & operator=(const Session &) = delete;

    /// Provides information about the authentication type of a specified user.
    std::unordered_set<AuthenticationType> getAuthenticationTypes(const String & user_name) const;

    /// Same as getAuthenticationType, but adds LoginFailure event in case of error.
    std::unordered_set<AuthenticationType> getAuthenticationTypesOrLogInFailure(const String & user_name) const;

    /// Sets the current user, checks the credentials and that the specified address is allowed to connect from.
    /// The function throws an exception if there is no such user or password is wrong.
    void authenticate(const String & user_name, const String & password, const Poco::Net::SocketAddress & address);
    void authenticate(const Credentials & credentials_, const Poco::Net::SocketAddress & address_);

    // Verifies whether the user's validity extends beyond the current time.
    // Throws an exception if the user's validity has expired.
    void checkIfUserIsStillValid();

    /// Writes a row about login failure into session log (if enabled)
    void onAuthenticationFailure(const std::optional<String> & user_name, const Poco::Net::SocketAddress & address_, const Exception & e);

    /// Returns a reference to the session's ClientInfo.
    const ClientInfo & getClientInfo() const;

    /// Modify the session's ClientInfo.
    void setClientInfo(const ClientInfo & client_info);
    void setClientName(const String & client_name);
    void setClientInterface(ClientInfo::Interface interface);
    void setClientVersion(UInt64 client_version_major, UInt64 client_version_minor, UInt64 client_version_patch, unsigned client_tcp_protocol_version);
    void setClientConnectionId(uint32_t connection_id);
    void setHTTPClientInfo(const Poco::Net::HTTPRequest & request);
    void setForwardedFor(const String & forwarded_for);
    void setQuotaClientKey(const String & quota_key);
    void setConnectionClientVersion(UInt64 client_version_major, UInt64 client_version_minor, UInt64 client_version_patch, unsigned client_tcp_protocol_version);

    const OpenTelemetry::TracingContext & getClientTraceContext() const;
    OpenTelemetry::TracingContext & getClientTraceContext();

    /// Makes a session context, can be used one or zero times.
    /// The function also assigns an user to this context.
    ContextMutablePtr makeSessionContext();
    ContextMutablePtr makeSessionContext(const String & session_name_, std::chrono::steady_clock::duration timeout_, bool session_check_);
    ContextMutablePtr sessionContext() { return session_context; }
    ContextPtr sessionContext() const { return session_context; }

    ContextPtr  sessionOrGlobalContext() const { return session_context ? session_context : global_context; }

    /// Makes a query context, can be used multiple times, with or without makeSession() called earlier.
    /// The query context will be created from a copy of a session context if it exists, or from a copy of
    /// a global context otherwise. In the latter case the function also assigns an user to this context.
    ContextMutablePtr makeQueryContext() const { return makeQueryContext(getClientInfo()); }
    ContextMutablePtr makeQueryContext(const ClientInfo & query_client_info) const;
    ContextMutablePtr makeQueryContext(ClientInfo && query_client_info) const;

    /// Releases the currently used session ID so it becomes available for reuse by another session.
    void releaseSessionID();

    /// Closes and removes session
    void closeSession(const String & session_id);
private:
    std::shared_ptr<SessionLog> getSessionLog() const;
    ContextMutablePtr makeQueryContextImpl(const ClientInfo * client_info_to_copy, ClientInfo * client_info_to_move) const;
    void recordLoginSuccess(ContextPtr login_context) const;

    mutable bool notified_session_log_about_login = false;
    const UUID auth_id;
    const ContextPtr global_context;

    /// ClientInfo that will be copied to a session context when it's created.
    std::optional<ClientInfo> prepared_client_info;

    mutable UserPtr user;
    std::optional<UUID> user_id;
    AuthenticationData user_authenticated_with;

    ContextMutablePtr session_context;
    mutable bool query_context_created = false;

    std::shared_ptr<NamedSessionData> named_session;
    bool named_session_created = false;

    SessionTracker::SessionTrackerHandle session_tracker_handle;

    /// Settings received from authentication server during authentication process
    /// to set when creating a session context
    SettingsChanges settings_from_auth_server;

    LoggerPtr log = nullptr;
};

}
