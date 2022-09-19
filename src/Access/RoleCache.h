#pragma once

#include <Access/EnabledRoles.h>
#include <Poco/AccessExpireCache.h>
#include <boost/container/flat_set.hpp>
#include <map>
#include <mutex>


namespace DB
{
class AccessControl;
struct Role;
using RolePtr = std::shared_ptr<const Role>;

class RoleCache
{
public:
    explicit RoleCache(const AccessControl & access_control_);
    ~RoleCache();

    std::shared_ptr<const EnabledRoles> getEnabledRoles(
        const std::vector<UUID> & current_roles,
        const std::vector<UUID> & current_roles_with_admin_option);

private:
    void collectEnabledRoles(scope_guard * notifications);
    void collectEnabledRoles(EnabledRoles & enabled, scope_guard * notifications);
    RolePtr getRole(const UUID & role_id);
    void roleChanged(const UUID & role_id, const RolePtr & changed_role);
    void roleRemoved(const UUID & role_id);

    const AccessControl & access_control;
    Poco::AccessExpireCache<UUID, std::pair<RolePtr, scope_guard>> cache;
    std::map<EnabledRoles::Params, std::weak_ptr<EnabledRoles>> enabled_roles;
    mutable std::mutex mutex;
};

}
