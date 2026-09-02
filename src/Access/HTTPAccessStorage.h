#pragma once

#include <atomic>
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
    /// forwards to `isEphemeral`. (Verified on `origin/master`.)
    bool isEphemeral() const override { return true; }
    bool exists(const UUID & id) const override;

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
    /// ephemeral user if needed (subject to the max_cached_users soft bound).
    UUID getOrCreateUser(const String & user_name) const;

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

    /// Configuration; immutable after construction (no reload support in v1).
    String http_auth_server_name;
    std::unordered_set<String> allowed_roles;
    std::vector<String> allowed_role_prefixes;
    String default_profile;
    AllowedClientHosts allowed_client_hosts = AllowedClientHosts::AnyHostTag{};
    size_t max_cached_users = 10000;

    /// Guards the whole resolve-then-find-then-reconcile-or-insert sequence of
    /// getOrCreateUser: concurrent first authentications of the same username converge on
    /// one cached entity, and the default_profile identity that is published — whether by
    /// inserting a new user or by reconciling a cached one — is never older than what a
    /// concurrent authentication has already resolved. Both effects need resolution AND
    /// its use in one critical section, not merely a lock around the write.
    /// Remote HTTP I/O is never performed under this mutex: authentication against the
    /// external server has already completed by the time getOrCreateUser is called, and
    /// everything inside is an in-memory AccessControl/MemoryAccessStorage lookup, so
    /// distinct usernames still authenticate fully concurrently (the ADR's requirement).
    /// The max_cached_users capacity is deliberately NOT observed under this mutex — it is
    /// a soft bound, observed on the cache size outside. Only the decision to apply that
    /// observation is inside, because it depends on the authoritative new-vs-cached
    /// lookup: a username that turns out to be already materialized is never rejected on
    /// the bound.
    /// MemoryAccessStorage is internally thread-safe; getOrCreateUser is its only writer
    /// here (the generic `removeReferencesToRemovedIDs` cascade cannot write to it: it
    /// goes through the read-only outer storage and fails with ACCESS_STORAGE_READONLY),
    /// so this mutex is sufficient to make the reconciliation a true compare-and-set.
    /// One case bypasses this mutex entirely: a cache hit when no `default_profile` is
    /// configured, where there is nothing to reconcile and the cached entity is a pure
    /// function of immutable configuration — see the guard at the top of getOrCreateUser
    /// for the invariants that make it safe and the condition for deleting it.
    mutable std::mutex mutex;
    mutable MemoryAccessStorage memory_storage;

    /// Count of users materialized so far, maintained instead of querying
    /// `memory_storage.findAll<User>().size` on every first-time authentication:
    /// `MemoryAccessStorage::findAllImpl` walks every cached entry and builds a vector, so
    /// with a large cache that query would do O(cache size) work on the hot path merely to
    /// learn a count. Incremented once, right after a successful `memory_storage.insert` in
    /// `getOrCreateUser` (the only inserter). `memory_order_relaxed` is sufficient on both
    /// the increment and the load in `getOrCreateUser`: this is a capacity statistic feeding
    /// a soft, approximate bound, not the synchronization mechanism for the cached entity
    /// itself — the mutex above already provides that. Also used by the destructor instead
    /// of `findAll<User>().size` (Task 11).
    mutable std::atomic<size_t> cached_user_count{0};
};

}
