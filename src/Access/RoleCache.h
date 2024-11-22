#pragma once

#include <map>
#include <mutex>
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

private:
    using SubscriptionsOnRoles = std::vector<std::shared_ptr<scope_guard>>;

    void collectEnabledRoles(scope_guard * notifications) TSA_REQUIRES(mutex);
    void collectEnabledRoles(EnabledRoles & enabled_roles, SubscriptionsOnRoles & subscriptions_on_roles, scope_guard * notifications) TSA_REQUIRES(mutex);
    RolePtr getRole(const UUID & role_id, SubscriptionsOnRoles & subscriptions_on_roles) TSA_REQUIRES(mutex);
    void roleChanged(const UUID & role_id, const RolePtr & changed_role);
    void roleRemoved(const UUID & role_id);

    const AccessControl & access_control;

    Poco::AccessExpireCache<UUID, std::pair<RolePtr, std::shared_ptr<scope_guard>>> TSA_GUARDED_BY(mutex) cache;

    struct EnabledRolesWithSubscriptions
    {
        std::weak_ptr<EnabledRoles> enabled_roles;

        /// We need to keep subscriptions for all enabled roles to be able to recalculate EnabledRolesInfo when some of the roles change.
        /// `cache` also keeps subscriptions but that's not enough because values can be purged from the `cache` anytime.
        SubscriptionsOnRoles subscriptions_on_roles;
    };

    std::map<EnabledRoles::Params, EnabledRolesWithSubscriptions> TSA_GUARDED_BY(mutex) enabled_roles_by_params;

    mutable std::mutex mutex;
};

}
