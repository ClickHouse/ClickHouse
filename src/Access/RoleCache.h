#pragma once

#include <Access/EnabledRoles.h>
#include <Poco/ExpireCache.h>
#include <map>
#include <mutex>


namespace DB
{
class AccessControlManager;
struct Role;
using RolePtr = std::shared_ptr<const Role>;

class RoleCache
{
public:
    RoleCache(const AccessControlManager & manager_);
    ~RoleCache();

    std::shared_ptr<const EnabledRoles> getEnabledRoles(const std::vector<UUID> & current_roles, const std::vector<UUID> & current_roles_with_admin_option);

private:
    void collectRolesInfo();
    void collectRolesInfoFor(EnabledRoles & enabled);
    RolePtr getRole(const UUID & role_id);
    void roleChanged(const UUID & role_id, const RolePtr & changed_role);
    void roleRemoved(const UUID & role_id);

    const AccessControlManager & manager;
    Poco::ExpireCache<UUID, std::pair<RolePtr, ext::scope_guard>> cache;
    std::map<EnabledRoles::Params, std::weak_ptr<EnabledRoles>> enabled_roles;
    mutable std::mutex mutex;
};

}
