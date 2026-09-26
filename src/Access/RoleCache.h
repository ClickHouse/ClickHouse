#pragma once

#include <map>
#include <mutex>
#include <unordered_set>
#include <Access/EnabledRoles.h>
#include <Poco/AccessExpireCache.h>


namespace DB
{
class AccessControl;
struct Role;
using RolePtr = std::shared_ptr<const Role>;

class RoleCache
{
public:
    explicit RoleCache(const AccessControl & access_control_, int expiration_time_seconds);
    ~RoleCache();

    std::shared_ptr<const EnabledRoles> getEnabledRoles(
        const std::vector<UUID> & current_roles,
        const std::vector<UUID> & current_roles_with_admin_option);

    std::shared_ptr<const EnabledRoles> getEnabledRoles(
        boost::container::flat_set<UUID> current_roles,
        boost::container::flat_set<UUID> current_roles_with_admin_option);

private:
    void ensureSubscribed() TSA_REQUIRES(mutex);
    void collectEnabledRolesIfNeeded(scope_guard * notifications) TSA_REQUIRES(mutex);
    void collectEnabledRoles(scope_guard * notifications) TSA_REQUIRES(mutex);
    void collectEnabledRoles(EnabledRoles & enabled_roles, scope_guard * notifications) TSA_REQUIRES(mutex);
    RolePtr getRole(const UUID & role_id) TSA_REQUIRES(mutex);
    void roleChanged(const UUID & role_id, const RolePtr & changed_role) TSA_REQUIRES(mutex);
    void roleRemoved(const UUID & role_id) TSA_REQUIRES(mutex);

    const AccessControl & access_control;
    scope_guard subscription;
    bool subscribed TSA_GUARDED_BY(mutex) = false;

    /// Set while applying a batch of changes; the recalculation is coalesced to once per notification batch.
    bool need_collect_enabled_roles TSA_GUARDED_BY(mutex) = false;

    Poco::AccessExpireCache<UUID, RolePtr> TSA_GUARDED_BY(mutex) cache;

    std::map<EnabledRoles::Params, std::weak_ptr<EnabledRoles>> TSA_GUARDED_BY(mutex) enabled_roles_by_params;

    /// Roles that take part in some enabled set: a change to a role outside this set needs no recalculation.
    std::unordered_set<UUID> TSA_GUARDED_BY(mutex) referenced_roles;

    mutable std::mutex mutex;
};

}
