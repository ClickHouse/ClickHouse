#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/scope_guard.h>
#include <base/types.h>

#include <memory>
#include <vector>


namespace DB
{

/// Maintains the warning in `system.warnings` that users can connect from any host without a password while the server
/// authenticates users on a non-loopback address. Only the users whose names a client does not need to guess are checked:
/// the well-known `default` and the users that connections without a user name get (`default_session_user`, globally or per
/// endpoint of the `protocols` section). A user restricted by `networks` / `HOST` (e.g. to localhost) is a deliberate setup.
///
/// The warning is re-checked when a user changes (users.xml reload, `ALTER USER`, ...), when the listeners change
/// (`setListeners`), and when the `VALID UNTIL` of a reported login passes. A failed check is retried on the next access change.
/// After `TRUNCATE TABLE system.warnings` the warning stays hidden until its text changes, i.e. until the listen addresses or
/// the reported users change, or until the server restarts.
class UsersWithoutPasswordWarning
{
public:
    /// A listening socket that authenticates the users of its connections (not one that only exposes metrics, for example).
    struct Listener
    {
        String host;
        /// The user that a connection without a user name gets; empty if such connections are rejected.
        String default_session_user;
    };

    /// Subscribes to the changes of users. Nothing is reported until `setListeners` is called.
    explicit UsersWithoutPasswordWarning(ContextPtr global_context);

    /// Unsubscribes and stops the scheduled re-check. Must be destroyed before the global context is shut down.
    ~UsersWithoutPasswordWarning();

    UsersWithoutPasswordWarning(const UsersWithoutPasswordWarning &) = delete;
    UsersWithoutPasswordWarning & operator=(const UsersWithoutPasswordWarning &) = delete;

    /// Replaces the listeners and re-checks. Loopback listeners are ignored. Never throws.
    void setListeners(const std::vector<Listener> & listeners);

    /// Whether the address a server listens on is a loopback one. A host name other than `localhost` is not resolved and
    /// counts as non-loopback.
    static bool isLoopbackHost(const String & host);

private:
    struct State;
    std::shared_ptr<State> state;
    scope_guard subscription;
};

}
