#pragma once

#include <mutex>
#include <optional>
#include <unordered_set>

#include <Access/Common/AllowedClientHosts.h>
#include <Access/IAccessStorage.h>
#include <Access/MemoryAccessStorage.h>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
class AccessControl;

/// Implementation of IAccessStorage which authenticates users through an external
/// HTTP authentication server (an `http_authentication_servers` entry referenced by
/// name) and materializes successfully authenticated users as ephemeral in-memory
/// entities. Helper-returned roles are session-scoped and are never persisted as
/// grants on the cached user. Nothing is persisted to disk; the configuration is
/// fixed until server restart.
class HTTPAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "http";

    HTTPAccessStorage(
        const String & storage_name_,
        AccessControl & access_control_,
        const Poco::Util::AbstractConfiguration & config,
        const String & prefix);

    ~HTTPAccessStorage() override;

    const char * getStorageType() const override { return STORAGE_TYPE; }
    bool isReadOnly() const override { return true; }
    /// Users materialized by this directory are ephemeral: in-memory only, never backed
    /// up, gone on restart. `MemoryAccessStorage` (which this class composes rather than
    /// inherits) already reports true; composition does not forward it, and the default
    /// on `IAccessStorage` is false.
    /// The consumer that matters is `InterpreterCreateQuery`'s `DEFINER` handling, which
    /// calls `access_control.isEphemeral(access_control.getID<User>(definer_name))` — the
    /// per-id overload — to decide whether to snapshot `CREATE VIEW ... DEFINER = <user>`
    /// into a persistent shadow user instead of naming a user that may not exist after a
    /// restart. Overriding only this no-argument form is SUFFICIENT and no per-id override
    /// is needed: `MultipleAccessStorage::isEphemeral(id)` resolves the owning storage and
    /// calls its `isEphemeral(id)`, and the base `IAccessStorage::isEphemeral(const UUID &)`
    /// forwards to `isEphemeral`.
    bool isEphemeral() const override { return true; }
    bool exists(const UUID & id) const override;

    /// `SYSTEM RELOAD USERS` (`ReloadMode::ALL`) drops every materialized user: the
    /// `max_cached_users` bound becomes recoverable without a restart, and a name held by a
    /// materialized user is released for `CREATE USER`. Established sessions of dropped users
    /// fail on their next query and must re-authenticate (HTTP requests re-authenticate on
    /// their own; a named HTTP session is recreated under the new user id). The users.xml-only
    /// reload mode does not touch the cache. The configuration itself is not reloaded.
    void reload(ReloadMode reload_mode) override;

    const String & getHTTPAuthServerName() const { return http_auth_server_name; }

private: // IAccessStorage implementations.
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<std::pair<String, AccessEntityType>> readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<AuthResult> authenticateImpl(
        const Credentials & credentials,
        const Poco::Net::IPAddress & address,
        const ExternalAuthenticators & external_authenticators,
        const ClientInfo & client_info,
        bool throw_if_user_not_exists,
        bool allow_no_password,
        bool allow_plaintext_password) const override;

    // Below: not IAccessStorage implementations.
    void setConfiguration(const Poco::Util::AbstractConfiguration & config, const String & prefix);

    /// Throws if the role name is not delegated to this directory by
    /// allowed_roles / allowed_role_prefix (fail-closed: an empty delegation
    /// configuration denies every helper-returned role).
    void checkRoleIsAllowed(const String & role_name) const;

    /// Returns the id of the cached user with this name, materializing a new
    /// ephemeral user if needed (subject to the max_cached_users bound).
    UUID getOrCreateUser(const String & user_name) const;

    /// The HTTP authentication method every user of this directory carries, with the
    /// per-authentication expiry when the response returned one.
    AuthenticationData makeAuthenticationData(time_t valid_until = 0) const;

    /// Resolves the configured `default_profile` to the UUID it currently names, or
    /// nullopt when no `default_profile` is configured. Throws (fail-closed) when a
    /// configured name does not resolve. An in-memory `AccessControl::find` lookup.
    std::optional<UUID> resolveDefaultProfile() const;

    /// Corrects the cached user's `parent_profile` reference in place when it differs
    /// from `profile_id`. A no-op in the common case. `mutex` MUST be held by the caller,
    /// together with the `resolveDefaultProfile` call that produced `profile_id`: this is
    /// a compare-and-set against a resolution instant, so a resolution obtained outside
    /// the critical section could overwrite a newer reconciliation with an older identity.
    void reconcileCachedProfile(const UUID & id, const std::optional<UUID> & profile_id) const;

    AccessControl & access_control;

    /// Configuration; immutable after construction (`reload` clears the cache, it does not reconfigure).
    String http_auth_server_name;
    std::unordered_set<String> allowed_roles;
    std::vector<String> allowed_role_prefixes;
    String default_profile;
    AllowedClientHosts allowed_client_hosts = AllowedClientHosts::AnyHostTag{};
    size_t max_cached_users = 10000;

    /// Serializes `getOrCreateUser` and `reload`, the only writers of `memory_storage`
    /// (the generic dependency cleanup cannot write through the read-only outer storage).
    /// No remote I/O is performed under it.
    mutable std::mutex mutex;
    mutable MemoryAccessStorage memory_storage;
    /// Number of materialized users; cheaper than counting `memory_storage` on every
    /// first authentication. Guarded by `mutex`.
    mutable size_t cached_user_count = 0;
};

}
