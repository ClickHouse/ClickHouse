#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Access/LDAPClient.h>
#include <Access/Credentials.h>
#include <Common/ThreadPool_fwd.h>
#include <base/types.h>
#include <base/scope_guard.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_set>
#include <vector>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{
class AccessControl;

/// Implementation of IAccessStorage which allows attaching users from a remote LDAP server.
/// Currently, any user name will be treated as a name of an existing remote user,
/// a user info entity will be created, with LDAP authentication type.
/// Names listed in `exclude_users` are the exception: this storage reports them as not found
/// without contacting the LDAP server, so that the storages that follow can serve them.
///
/// With a `<sync>` section the storage is additionally populated proactively: a background job
/// enumerates the directory under the server's lookup identity and applies the result to the
/// same in-memory storage the lazy path uses (see `sync`). In such a "synced" directory the
/// synchronisation is the sole role authority: logins only verify the password. A synced directory
/// must be the only `ldap` directory of the server (see `AccessControl::checkLDAPStoragesLayout`).
class LDAPAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "ldap";

    explicit LDAPAccessStorage(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix);
    ~LDAPAccessStorage() override;

    String getLDAPServerName() const;

    /// Parameters of the proactive synchronisation, the `<sync>` section of the directory.
    struct SyncParams
    {
        /// Time between two runs; 0 = run once at startup and on `SYSTEM RELOAD USERS` only.
        std::chrono::seconds interval{900};
        /// The search that lists the users and the attribute holding their names; `max_entries` = `max_users`.
        LDAPClient::UserEnumerationParams enumeration;
        /// Create the roles named by the `groups` allow-lists of the role mappings when they do not exist.
        bool create_roles = false;
        /// Storage the roles are created in; empty = the first writable storage, which must not be ephemeral.
        String roles_storage;
        /// Serve only the users the last synchronisation materialised; a name outside the snapshot is "not found".
        bool only_synced_users = true;
        /// A run that finds fewer users applies nothing.
        size_t min_users = 1;
        /// A run that receives more entries applies nothing; 0 = unlimited.
        size_t max_users = 10000;
        /// A run that would remove more than max(1, floor(fraction * synced users)) users applies nothing.
        double max_removed_fraction = 0.5;
        /// Refuse logins of synced users when the last successful run is older than this; 0 = off.
        std::chrono::seconds max_staleness{0};
        /// Enumerate and plan, log what would change, apply nothing.
        bool dry_run = false;
    };

    // IAccessStorage implementations.
    const char * getStorageType() const override;
    String getStorageParamsJSON() const override;
    bool isReadOnly() const override { return true; }
    bool exists(const UUID & id) const override;

    /// The synchronisation job: started by `startPeriodicReloading` (first run after a jitter of up to
    /// `interval` / 10, then every `interval`, or min(`interval`, 60 s) after a failed run), stopped
    /// by `stopPeriodicReloading` and `shutdown`. `reload(ReloadMode::ALL)` (`SYSTEM RELOAD USERS`)
    /// runs one synchronisation in the caller's thread and propagates its error; runs are serialised.
    /// All four are no-ops without a `<sync>` section.
    void startPeriodicReloading() override;
    void stopPeriodicReloading() override;
    /// Non-virtual worker shared by `stopPeriodicReloading`, `shutdown` and the destructor
    /// (a destructor must not reach the thread through a virtual call).
    void stopSyncThread();
    void shutdown() override;
    void reload(ReloadMode reload_mode) override;
    bool reloadsAfterOtherStorages() const override { return sync_params.has_value(); }

    /// Whether the directory has a `<sync>` section. `sync_params` is set by the constructor and never changes.
    bool hasSync() const { return sync_params.has_value(); }

private: // IAccessStorage implementations.
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::optional<UUID> findImpl(AccessEntityType type, const String & name, bool force_external_lookup) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<std::pair<String, AccessEntityType>> readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<AuthResult> authenticateImpl(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators, const ClientInfo & client_info, bool throw_if_user_not_exists, bool allow_no_password, bool allow_plaintext_password) const override;

    void setConfiguration(const Poco::Util::AbstractConfiguration & config, const String & prefix);
    static SyncParams parseSyncParams(const Poco::Util::AbstractConfiguration & config, const String & prefix, const LDAPClient::RoleSearchParamsList & role_search_params);
    void processRoleChange(const UUID & id, const AccessEntityPtr & entity);

    /// `role_id` is taken by value: a caller may pass a reference into `granted_role_ids`, which the revoke
    /// branch erases before it uses `role_id`.
    void applyRoleChangeNoLock(bool grant, UUID role_id, const String & role_name);
    void grantRoleByNameNoLock(const UUID & id, const String & role_name);
    void assignRolesNoLock(User & user, const LDAPClient::SearchResultsList & external_roles) const;
    void updateAssignedRolesNoLock(const UUID & id, const String & user_name, const LDAPClient::SearchResultsList & external_roles) const;
    /// Forgets everything `assignRolesNoLock` recorded about a user that is being removed from `memory_storage`.
    void removeUserNoLock(const String & user_name) const;
    std::set<String> mapExternalRolesNoLock(const LDAPClient::SearchResultsList & external_roles) const;
    bool areLDAPCredentialsValidNoLock(const User & user, const Credentials & credentials,
        const ExternalAuthenticators & external_authenticators, LDAPClient::SearchResultsList & role_search_results) const;
    std::shared_ptr<User> makeUserNoLock(const String & user_name) const;

    /// Throws `LDAP_ERROR` when `max_staleness` is set and the last successful synchronisation is older than it.
    /// Only called for names present in `memory_storage`, so it can never refuse a user of another storage.
    /// `action` names the refused operation in the message: "authenticate" for a login, "resolve" for the
    /// forced lookup of `EXECUTE AS`.
    void checkNotStale(const String & user_name, std::string_view action) const;

    /// The synchronisation, see `sync`.
    struct SyncPlan
    {
        /// The users to materialise, by name, after the duplicate, `exclude_users` and shadow rules.
        std::map<String, LDAPSyncClient::UserEntry> users;
        size_t entries = 0;
        size_t excluded = 0;
        size_t shadowed = 0;
    };

    /// What `applySyncPlanNoLock` will do to `memory_storage`, computed against its current content.
    struct SyncDiff
    {
        std::vector<const LDAPSyncClient::UserEntry *> to_add;
        /// Present already, with a different set of external roles.
        std::vector<std::pair<UUID, const LDAPSyncClient::UserEntry *>> to_update;
        /// Materialised by an earlier run and absent from the plan.
        std::vector<std::pair<UUID, String>> to_remove;
        /// Number of users the earlier runs materialised (the denominator of `max_removed_fraction`).
        size_t synced = 0;
    };

    struct SyncApplyResult
    {
        size_t added = 0;
        size_t updated = 0;
        size_t removed = 0;
        /// Ids of the removed users, for the cleanup of the references other storages hold to them (see `sync`).
        std::unordered_set<UUID> removed_ids;
        std::set<String> missing_roles;
    };

    void runSyncThread();
    void sync();
    SyncPlan planSync(std::vector<LDAPSyncClient::UserEntry> entries) const;
    SyncDiff computeSyncDiffNoLock(const SyncPlan & plan) const;
    void checkRemovalGuard(const SyncDiff & diff) const;
    std::set<String> getAllowListedRoleNames() const;
    std::shared_ptr<IAccessStorage> selectRolesStorage();
    size_t createMissingRoles(const std::set<String> & role_names, IAccessStorage & storage);
    SyncApplyResult applySyncPlanNoLock(const SyncPlan & plan, const SyncDiff & diff);
    void logDryRun(const SyncPlan & plan, const SyncDiff & diff, const std::set<String> & roles_to_create, const IAccessStorage * roles_target) const;

    mutable std::recursive_mutex mutex; // Note: Reentrace possible by internal role lookup via access_control
    AccessControl & access_control;
    String ldap_server_name;
    LDAPClient::RoleSearchParamsList role_search_params;
    std::set<String> common_role_names;                         // role name that should be granted to all users at all times
    std::set<String> excluded_user_names;                       // user names this storage never serves (`exclude_users`)
    std::optional<SyncParams> sync_params;                      // set iff the directory has a `<sync>` section
    mutable std::map<String, LDAPClient::SearchResultsList> users_external_roles; // user name -> LDAPClient::SearchResultsList (most recently retrieved and processed)
    mutable std::set<String> unverified_user_names;             // user names materialised by interserver `AlwaysAllowCredentials` and not confirmed by the directory yet
    mutable std::map<String, std::set<String>> users_per_roles; // role name -> user names (...it should be granted to; may but don't have to exist for common roles)
    mutable std::map<String, std::set<String>> roles_per_users; // user name -> role names (...that should be granted to it; may but don't have to include common roles)
    mutable std::map<UUID, String> granted_role_names;          // (currently granted) role id -> its name
    mutable std::map<String, UUID> granted_role_ids;            // (currently granted) role name -> its id
    mutable std::set<String> synced_user_names;                 // user names the synchronisation materialised (removed by it when they leave the directory)
    scope_guard role_change_subscription;
    mutable MemoryAccessStorage memory_storage;

    /// The synchronisation job. `sync_thread_mutex` guards the thread handle, the exit flag and the
    /// condition variable the thread waits on between runs; `sync_mutex` serialises the runs themselves
    /// (the thread and `SYSTEM RELOAD USERS`). Neither is ever held together with `mutex` for long:
    /// a run takes `mutex` only for the in-memory apply phase.
    std::unique_ptr<ThreadFromGlobalPool> sync_thread;
    std::mutex sync_thread_mutex;
    std::condition_variable sync_thread_cv;
    bool sync_thread_should_exit = false;
    std::mutex sync_mutex;
    bool roles_storage_pick_logged = false;                     // the ambiguous default pick is warned about once, under `sync_mutex`
    /// Steady-clock seconds of the last successful (non-dry) run, 0 = never; read by `checkNotStale` without `mutex`.
    std::atomic<Int64> last_sync_success_time_s{0};
    mutable std::atomic<Int64> last_staleness_log_time_s{0};    // once-per-minute throttle of the staleness warning
};
}
