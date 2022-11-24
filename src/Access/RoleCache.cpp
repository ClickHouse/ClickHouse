#include <Access/RoleCache.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/AccessControl.h>
#include <boost/container/flat_set.hpp>
#include <base/FnTraits.h>


namespace DB
{
namespace
{
    void collectRoles(EnabledRolesInfo & roles_info,
                      boost::container::flat_set<UUID> & skip_ids,
                      Fn<RolePtr(const UUID &)> auto && get_role_function,
                      const UUID & role_id,
                      bool is_current_role,
                      bool with_admin_option)
    {
        if (roles_info.enabled_roles.count(role_id))
        {
            if (is_current_role)
                roles_info.current_roles.emplace(role_id);
            if (with_admin_option)
                roles_info.enabled_roles_with_admin_option.emplace(role_id);
            return;
        }

        if (skip_ids.count(role_id))
            return;

        auto role = get_role_function(role_id);

        if (!role)
        {
            skip_ids.emplace(role_id);
            return;
        }

        roles_info.enabled_roles.emplace(role_id);
        if (is_current_role)
            roles_info.current_roles.emplace(role_id);
        if (with_admin_option)
            roles_info.enabled_roles_with_admin_option.emplace(role_id);

        roles_info.names_of_roles[role_id] = role->getName();
        roles_info.access.makeUnion(role->access);
        roles_info.settings_from_enabled_roles.merge(role->settings);

        for (const auto & granted_role : role->granted_roles.getGranted())
            collectRoles(roles_info, skip_ids, get_role_function, granted_role, false, false);

        for (const auto & granted_role : role->granted_roles.getGrantedWithAdminOption())
            collectRoles(roles_info, skip_ids, get_role_function, granted_role, false, true);
    }
}


RoleCache::RoleCache(const AccessControl & access_control_)
    : access_control(access_control_), cache(600000 /* 10 minutes */) {}


RoleCache::~RoleCache() = default;


std::shared_ptr<const EnabledRoles>
RoleCache::getEnabledRoles(const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option)
{
    /// Declared before `lock` to send notifications after the mutex will be unlocked.
    scope_guard notifications;

    std::lock_guard lock{mutex};
    EnabledRoles::Params params;
    params.current_roles.insert(roles.begin(), roles.end());
    params.current_roles_with_admin_option.insert(roles_with_admin_option.begin(), roles_with_admin_option.end());
    auto it = enabled_roles.find(params);
    if (it != enabled_roles.end())
    {
        auto from_cache = it->second.lock();
        if (from_cache)
            return from_cache;
        enabled_roles.erase(it);
    }

    auto res = std::shared_ptr<EnabledRoles>(new EnabledRoles(params));
    collectEnabledRoles(*res, notifications);
    enabled_roles.emplace(std::move(params), res);
    return res;
}


void RoleCache::collectEnabledRoles(scope_guard & notifications)
{
    /// `mutex` is already locked.

    for (auto i = enabled_roles.begin(), e = enabled_roles.end(); i != e;)
    {
        auto elem = i->second.lock();
        if (!elem)
            i = enabled_roles.erase(i);
        else
        {
            collectEnabledRoles(*elem, notifications);
            ++i;
        }
    }
}


void RoleCache::collectEnabledRoles(EnabledRoles & enabled, scope_guard & notifications)
{
    /// `mutex` is already locked.

    /// Collect enabled roles. That includes the current roles, the roles granted to the current roles, and so on.
    auto new_info = std::make_shared<EnabledRolesInfo>();
    boost::container::flat_set<UUID> skip_ids;

    auto get_role_function = [this](const UUID & id) { return getRole(id); };

    for (const auto & current_role : enabled.params.current_roles)
        collectRoles(*new_info, skip_ids, get_role_function, current_role, true, false);

    for (const auto & current_role : enabled.params.current_roles_with_admin_option)
        collectRoles(*new_info, skip_ids, get_role_function, current_role, true, true);

    /// Collect data from the collected roles.
    enabled.setRolesInfo(new_info, notifications);
}


RolePtr RoleCache::getRole(const UUID & role_id)
{
    /// `mutex` is already locked.

    auto role_from_cache = cache.get(role_id);
    if (role_from_cache)
        return role_from_cache->first;

    auto subscription = access_control.subscribeForChanges(role_id,
                                                    [this, role_id](const UUID &, const AccessEntityPtr & entity)
    {
        auto changed_role = entity ? typeid_cast<RolePtr>(entity) : nullptr;
        if (changed_role)
            roleChanged(role_id, changed_role);
        else
            roleRemoved(role_id);
    });

    auto role = access_control.tryRead<Role>(role_id);
    if (role)
    {
        auto cache_value = Poco::SharedPtr<std::pair<RolePtr, scope_guard>>(
            new std::pair<RolePtr, scope_guard>{role, std::move(subscription)});
        cache.add(role_id, cache_value);
        return role;
    }

    return nullptr;
}


void RoleCache::roleChanged(const UUID & role_id, const RolePtr & changed_role)
{
    /// Declared before `lock` to send notifications after the mutex will be unlocked.
    scope_guard notifications;

    std::lock_guard lock{mutex};
    auto role_from_cache = cache.get(role_id);
    if (!role_from_cache)
        return;
    role_from_cache->first = changed_role;
    cache.update(role_id, role_from_cache);
    collectEnabledRoles(notifications);
}


void RoleCache::roleRemoved(const UUID & role_id)
{
    /// Declared before `lock` to send notifications after the mutex will be unlocked.
    scope_guard notifications;

    std::lock_guard lock{mutex};
    cache.remove(role_id);
    collectEnabledRoles(notifications);
}

}
