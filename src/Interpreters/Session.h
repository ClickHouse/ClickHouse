#pragma once

#include <Common/SettingsChanges.h>
#include <Access/Authentication.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>

#include <chrono>
#include <memory>
#include <optional>

namespace Poco::Net { class SocketAddress; }

namespace DB
{
class Credentials;
class Authentication;
struct NamedSessionData;
class NamedSessionsStorage;
struct User;
using UserPtr = std::shared_ptr<const User>;

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

    Session(const ContextPtr & global_context_, ClientInfo::Interface interface_);
    Session(Session &&);
    ~Session();

    Session(const Session &) = delete;
    Session& operator=(const Session &) = delete;

    /// Provides information about the authentication type of a specified user.
    Authentication::Type getAuthenticationType(const String & user_name) const;
    Authentication::Digest getPasswordDoubleSHA1(const String & user_name) const;

    /// Sets the current user, checks the credentials and that the specified address is allowed to connect from.
    /// The function throws an exception if there is no such user or password is wrong.
    void authenticate(const String & user_name, const String & password, const Poco::Net::SocketAddress & address);
    void authenticate(const Credentials & credentials_, const Poco::Net::SocketAddress & address_);

    /// Returns a reference to session ClientInfo.
    ClientInfo & getClientInfo();
    const ClientInfo & getClientInfo() const;

    /// Makes a session context, can be used one or zero times.
    /// The function also assigns an user to this context.
    ContextMutablePtr makeSessionContext();
    ContextMutablePtr makeSessionContext(const String & session_id_, std::chrono::steady_clock::duration timeout_, bool session_check_);
    ContextMutablePtr sessionContext() { return session_context; }
    ContextPtr sessionContext() const { return session_context; }

    /// Makes a query context, can be used multiple times, with or without makeSession() called earlier.
    /// The query context will be created from a copy of a session context if it exists, or from a copy of
    /// a global context otherwise. In the latter case the function also assigns an user to this context.
    ContextMutablePtr makeQueryContext() const { return makeQueryContext(getClientInfo()); }
    ContextMutablePtr makeQueryContext(const ClientInfo & query_client_info) const;
    ContextMutablePtr makeQueryContext(ClientInfo && query_client_info) const;

    /// Releases the currently used session ID so it becomes available for reuse by another session.
    void releaseSessionID();

private:
    ContextMutablePtr makeQueryContextImpl(const ClientInfo * client_info_to_copy, ClientInfo * client_info_to_move) const;

    const ContextPtr global_context;

    /// ClientInfo that will be copied to a session context when it's created.
    std::optional<ClientInfo> prepared_client_info;

    mutable UserPtr user;
    std::optional<UUID> user_id;

    ContextMutablePtr session_context;
    mutable bool query_context_created = false;

    String session_id;
    std::shared_ptr<NamedSessionData> named_session;
    bool named_session_created = false;
};

}
