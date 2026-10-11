#pragma once

#include <Core/UUID.h>
#include <Access/ImplicitExpansionSettings.h>
#include <base/defines.h>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{

struct User;
using UserPtr = std::shared_ptr<const User>;
struct EnabledRolesInfo;
class AccessRights;

/// Caches the effective access rights of a user with their enabled roles
/// (i.e. what ContextAccess::calculateAccessRights computes) so that all sessions
/// of the same user share one calculation instead of doing one per session.
///
/// The key is the identity of the immutable inputs: the exact User snapshot, the exact
/// EnabledRolesInfo snapshot, and the server settings the implicit expansion depends on.
/// Any change to the user or to a role produces new snapshots, so a hit always means the
/// inputs are the very same objects - no explicit invalidation is needed.
///
/// Entries hold only weak references to the results, like the other *Cache classes in this
/// directory: an entry stays valid exactly while some session still uses its results, and
/// dead entries are swept periodically, so nothing is retained for users without sessions.
/// (The user snapshot itself is NOT a liveness criterion - the access storage keeps the
/// current entity alive regardless of sessions.)
class EffectiveAccessRightsCache
{
public:
    struct Result
    {
        std::shared_ptr<const AccessRights> access;
        std::shared_ptr<const AccessRights> access_with_implicit;
    };

    /// Returns the result previously stored for exactly these snapshots and settings, or nullopt.
    std::optional<Result> find(const UUID & user_id, const UserPtr & user,
        const std::shared_ptr<const EnabledRolesInfo> & roles_info,
        const ImplicitExpansionSettings & settings);

    /// Stores the result calculated from these snapshots and settings,
    /// replacing any previous entry for the same snapshots of this user.
    void store(const UUID & user_id, const UserPtr & user,
        const std::shared_ptr<const EnabledRolesInfo> & roles_info,
        const ImplicitExpansionSettings & settings,
        const std::shared_ptr<const AccessRights> & access,
        const std::shared_ptr<const AccessRights> & access_with_implicit);

private:
    struct Entry
    {
        /// The exact snapshots the result was calculated from; weak, so that a user without
        /// sessions retains nothing.
        std::weak_ptr<const User> user;
        std::weak_ptr<const EnabledRolesInfo> roles_info;
        ImplicitExpansionSettings settings;

        std::weak_ptr<const AccessRights> access;
        std::weak_ptr<const AccessRights> access_with_implicit;
    };

    /// An entry is dead when no session holds its results anymore - a session holding the
    /// results also holds the snapshots they were calculated from.
    static bool isExpired(const Entry & entry) TSA_REQUIRES(mutex);

    /// Removes dead entries of all users. Runs after every SWEEP_INTERVAL stores; a sweep walks
    /// all entries under the mutex, so with a huge number of users it is a periodic spike rather
    /// than a strictly bounded cost per store. Accepted for now: entries die as soon as their
    /// sessions drop, so a sweep usually has little to remove and stays cheap.
    void sweepExpiredEntries() TSA_REQUIRES(mutex);

    static constexpr size_t SWEEP_INTERVAL = 1024;

    std::mutex mutex;
    /// One entry per live (user snapshot, enabled roles snapshot) combination: sessions of the
    /// same user can use different role sets (e.g. SET ROLE), and each combination keeps its
    /// own entry so the combinations don't evict each other.
    std::unordered_map<UUID, std::vector<Entry>> entries TSA_GUARDED_BY(mutex);
    size_t stores_since_sweep TSA_GUARDED_BY(mutex) = 0;
};

}
