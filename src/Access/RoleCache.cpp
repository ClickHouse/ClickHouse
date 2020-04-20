#include <Access/RoleCache.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/AccessControlManager.h>
#include <boost/container/flat_map.hpp>


namespace DB
{
namespace
{
    struct CollectedRoleInfo
    {
        RolePtr role;
        bool is_current_role = false;
        bool with_admin_option = false;
    };


    void collectRoles(boost::container::flat_map<UUID, CollectedRoleInfo> & collected_roles,
                      const std::function<RolePtr(const UUID &)> & get_role_function,
                      const UUID & role_id,
                      bool is_current_role,
                      bool with_admin_option)
    {
        auto it = collected_roles.find(role_id);
        if (it != collected_roles.end())
        {
            it->second.is_current_role |= is_current_role;
            it->second.with_admin_option |= with_admin_option;
            return;
        }

        auto role = get_role_function(role_id);
        collected_roles[role_id] = CollectedRoleInfo{role, is_current_role, with_admin_option};

        if (!role)
            return;

        for (const auto & granted_role : role->granted_roles.roles)
            collectRoles(collected_roles, get_role_function, granted_role, false, false);

        for (const auto & granted_role : role->granted_roles.roles_with_admin_option)
            collectRoles(collected_roles, get_role_function, granted_role, false, true);
    }


    std::shared_ptr<EnabledRolesInfo> collectInfoForRoles(const boost::container::flat_map<UUID, CollectedRoleInfo> & roles)
    {
        auto new_info = std::make_shared<EnabledRolesInfo>();
        for (const auto & [role_id, collect_info] : roles)
        {
            const auto & role = collect_info.role;
            if (!role)
                continue;
            if (collect_info.is_current_role)
                new_info->current_roles.emplace_back(role_id);
            new_info->enabled_roles.emplace_back(role_id);
            if (collect_info.with_admin_option)
                new_info->enabled_roles_with_admin_option.emplace_back(role_id);
            new_info->names_of_roles[role_id] = role->getName();
            new_info->access.merge(role->access.access);
            new_info->access_with_grant_option.merge(role->access.access_with_grant_option);
            new_info->settings_from_enabled_roles.merge(role->settings);
        }
        return new_info;
    }
}


RoleCache::RoleCache(const AccessControlManager & manager_)
    : manager(manager_), cache(600000 /* 10 minutes */) {}


RoleCache::~RoleCache() = default;


std::shared_ptr<const EnabledRoles> RoleCache::getEnabledRoles(
    const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option)
{
    std::lock_guard lock{mutex};

    EnabledRoles::Params params;
    params.current_roles = roles;
    params.current_roles_with_admin_option = roles_with_admin_option;
    auto it = enabled_roles.find(params);
    if (it != enabled_roles.end())
    {
        auto from_cache = it->second.lock();
        if (from_cache)
            return from_cache;
        enabled_roles.erase(it);
    }

    auto res = std::shared_ptr<EnabledRoles>(new EnabledRoles(params));
    collectRolesInfoFor(*res);
    enabled_roles.emplace(std::move(params), res);
    return res;
}


void RoleCache::collectRolesInfo()
{
    /// `mutex` is already locked.

    for (auto i = enabled_roles.begin(), e = enabled_roles.end(); i != e;)
    {
        auto elem = i->second.lock();
        if (!elem)
            i = enabled_roles.erase(i);
        else
        {
            collectRolesInfoFor(*elem);
            ++i;
        }
    }
}


void RoleCache::collectRolesInfoFor(EnabledRoles & enabled)
{
    /// `mutex` is already locked.

    /// Collect roles in use. That includes the current roles, the roles granted to the current roles, and so on.
    boost::container::flat_map<UUID, CollectedRoleInfo> collected_roles;
    auto get_role_function = [this](const UUID & id) { return getRole(id); };
    for (const auto & current_role : enabled.params.current_roles)
        collectRoles(collected_roles, get_role_function, current_role, true, false);

    for (const auto & current_role : enabled.params.current_roles_with_admin_option)
        collectRoles(collected_roles, get_role_function, current_role, true, true);

    /// Collect data from the collected roles.
    enabled.setRolesInfo(collectInfoForRoles(collected_roles));
}


RolePtr RoleCache::getRole(const UUID & role_id)
{
    /// `mutex` is already locked.

    auto role_from_cache = cache.get(role_id);
    if (role_from_cache)
        return role_from_cache->first;

    auto subscription = manager.subscribeForChanges(role_id,
                                                    [this, role_id](const UUID &, const AccessEntityPtr & entity)
    {
        auto changed_role = entity ? typeid_cast<RolePtr>(entity) : nullptr;
        if (changed_role)
            roleChanged(role_id, changed_role);
        else
            roleRemoved(role_id);
    });

    auto role = manager.tryRead<Role>(role_id);
    if (role)
    {
        auto cache_value = Poco::SharedPtr<std::pair<RolePtr, ext::scope_guard>>(
            new std::pair<RolePtr, ext::scope_guard>{role, std::move(subscription)});
        cache.add(role_id, cache_value);
        return role;
    }

    return nullptr;
}


void RoleCache::roleChanged(const UUID & role_id, const RolePtr & changed_role)
{
    std::lock_guard lock{mutex};
    auto role_from_cache = cache.get(role_id);
    if (!role_from_cache)
        return;
    role_from_cache->first = changed_role;
    cache.update(role_id, role_from_cache);
    collectRolesInfo();
}


void RoleCache::roleRemoved(const UUID & role_id)
{
    std::lock_guard lock{mutex};
    cache.remove(role_id);
    collectRolesInfo();
}

}
