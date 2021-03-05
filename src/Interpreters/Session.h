#pragma once

#include <common/types.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ClientInfo.h>

#include <chrono>
#include <memory>
#include <optional>

namespace Poco::Net { class SocketAddress; }

namespace DB
{
class Credentials;
class ContextAccess;
struct Settings;
class Authentication;
struct NamedSessionData;
class NamedSessionsStorage;

/** Represents user-session from the server perspective,
 *  basically it is just a smaller subset of Context API, simplifies Context management.
 *
 * Holds session context, facilitates acquisition of NamedSession and proper creation of query contexts.
 * Adds log in, log out and login failure events to the SessionLog.
 */
class Session
{
    static std::optional<NamedSessionsStorage> named_sessions;

public:
    /// Allow to use named sessions. The thread will be run to cleanup sessions after timeout has expired.
    /// The method must be called at the server startup.
    static void enableNamedSessions();

//    static Session makeSessionFromCopyOfContext(const ContextPtr & _context_to_copy);
    Session(const ContextPtr & context_to_copy, ClientInfo::Interface interface, std::optional<String> default_format = std::nullopt);
    virtual ~Session();

    Session(const Session &) = delete;
    Session& operator=(const Session &) = delete;

    Session(Session &&);
//    Session& operator=(Session &&);

    Authentication getUserAuthentication(const String & user_name) const;
    void setUser(const Credentials & credentials, const Poco::Net::SocketAddress & address);
    void setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address);

    /// Handle login and logout events.
    void onLogInSuccess();
    void onLogInFailure(const String & user_name, const std::exception & /* failure_reason */);
    void onLogOut();

    /** Propmotes current session to a named session.
     *
     *  that is: re-uses or creates NamedSession and then piggybacks on it's context,
     *  retaining ClientInfo of current session_context.
     *  Acquired named_session is then released in the destructor.
     */
    void promoteToNamedSession(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check);
    /// Early release a NamedSession.
    void releaseNamedSession();

    ContextMutablePtr makeQueryContext(const String & query_id) const;

    ContextPtr sessionContext() const;
    ContextMutablePtr mutableSessionContext();

    ClientInfo & getClientInfo();
    const ClientInfo & getClientInfo() const;

    const Settings & getSettings() const;

    void setQuotaKey(const String & quota_key);

    String getCurrentDatabase() const;
    void setCurrentDatabase(const String & name);

private:
    ContextMutablePtr session_context;
    // So that Session can be used after forced release of named_session.
    const ContextMutablePtr initial_session_context;
    std::shared_ptr<const ContextAccess> access;
    std::shared_ptr<NamedSessionData> named_session;
};

}
