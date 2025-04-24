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


RoleCache::RoleCache(const AccessControl & access_control_, int expiration_time_seconds)
    : access_control(access_control_), cache(expiration_time_seconds * 1000 /* 10 minutes by default*/)
{
}


RoleCache::~RoleCache() = default;


std::shared_ptr<const EnabledRoles>
RoleCache::getEnabledRoles(const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option)
{
    std::lock_guard lock{mutex};
    EnabledRoles::Params params;
    params.current_roles.insert(roles.begin(), roles.end());
    params.current_roles_with_admin_option.insert(roles_with_admin_option.begin(), roles_with_admin_option.end());
    auto it = enabled_roles_by_params.find(params);
    if (it != enabled_roles_by_params.end())
    {
        if (auto enabled_roles = it->second.enabled_roles.lock())
            return enabled_roles;
        enabled_roles_by_params.erase(it);
    }

    auto res = std::shared_ptr<EnabledRoles>(new EnabledRoles(params));
    SubscriptionsOnRoles subscriptions_on_roles;
    collectEnabledRoles(*res, subscriptions_on_roles, nullptr);
    enabled_roles_by_params.emplace(std::move(params), EnabledRolesWithSubscriptions{res, std::move(subscriptions_on_roles)});
    return res;
}


void RoleCache::collectEnabledRoles(scope_guard * notifications)
{
    /// `mutex` is already locked.

    for (auto i = enabled_roles_by_params.begin(), e = enabled_roles_by_params.end(); i != e;)
    {
        auto & item = i->second;
        if (auto enabled_roles = item.enabled_roles.lock())
        {
            collectEnabledRoles(*enabled_roles, item.subscriptions_on_roles, notifications);
            ++i;
        }
        else
        {
            i = enabled_roles_by_params.erase(i);
        }
    }
}


void RoleCache::collectEnabledRoles(EnabledRoles & enabled_roles, SubscriptionsOnRoles & subscriptions_on_roles, scope_guard * notifications)
{
    /// `mutex` is already locked.

    /// Collect enabled roles. That includes the current roles, the roles granted to the current roles, and so on.
    auto new_info = std::make_shared<EnabledRolesInfo>();
    boost::container::flat_set<UUID> skip_ids;

    /// We need to collect and keep not only enabled roles but also subscriptions for them to be able to recalculate EnabledRolesInfo when some of the roles change.
    SubscriptionsOnRoles new_subscriptions_on_roles;
    new_subscriptions_on_roles.reserve(subscriptions_on_roles.size());

    auto get_role_function = [this, &new_subscriptions_on_roles](const UUID & id) TSA_NO_THREAD_SAFETY_ANALYSIS { return getRole(id, new_subscriptions_on_roles); };

    for (const auto & current_role : enabled_roles.params.current_roles)
        collectRoles(*new_info, skip_ids, get_role_function, current_role, true, false);

    for (const auto & current_role : enabled_roles.params.current_roles_with_admin_option)
        collectRoles(*new_info, skip_ids, get_role_function, current_role, true, true);

    /// Remove duplicates from `subscriptions_on_roles`.
    std::sort(new_subscriptions_on_roles.begin(), new_subscriptions_on_roles.end());
    new_subscriptions_on_roles.erase(std::unique(new_subscriptions_on_roles.begin(), new_subscriptions_on_roles.end()), new_subscriptions_on_roles.end());
    subscriptions_on_roles = std::move(new_subscriptions_on_roles);

    /// Collect data from the collected roles.
    enabled_roles.setRolesInfo(new_info, notifications);
}


RolePtr RoleCache::getRole(const UUID & role_id, SubscriptionsOnRoles & subscriptions_on_roles)
{
    /// `mutex` is already locked.

    auto role_from_cache = cache.get(role_id);
    if (role_from_cache)
    {
        subscriptions_on_roles.emplace_back(role_from_cache->second);
        return role_from_cache->first;
    }

    auto on_role_changed_or_removed = [this, role_id](const UUID &, const AccessEntityPtr & entity)
    {
        auto changed_role = entity ? typeid_cast<RolePtr>(entity) : nullptr;
        if (changed_role)
            roleChanged(role_id, changed_role);
        else
            roleRemoved(role_id);
    };

    auto subscription_on_role = std::make_shared<scope_guard>(access_control.subscribeForChanges(role_id, on_role_changed_or_removed));

    auto role = access_control.tryRead<Role>(role_id);
    if (role)
    {
        auto cache_value = Poco::SharedPtr<std::pair<RolePtr, std::shared_ptr<scope_guard>>>(
            new std::pair<RolePtr, std::shared_ptr<scope_guard>>{role, subscription_on_role});
        cache.add(role_id, cache_value);
        subscriptions_on_roles.emplace_back(subscription_on_role);
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
    if (role_from_cache)
    {
        /// We update the role stored in a cache entry only if that entry has not expired yet.
        role_from_cache->first = changed_role;
        cache.update(role_id, role_from_cache);
    }

    /// An enabled role for some users has been changed, we need to recalculate the access rights.
    collectEnabledRoles(&notifications); /// collectEnabledRoles() must be called with the `mutex` locked.
}


void RoleCache::roleRemoved(const UUID & role_id)
{
    /// Declared before `lock` to send notifications after the mutex will be unlocked.
    scope_guard notifications;

    std::lock_guard lock{mutex};

    /// If a cache entry with the role has expired already, that remove() will do nothing.
    cache.remove(role_id);

    /// An enabled role for some users has been removed, we need to recalculate the access rights.
    collectEnabledRoles(&notifications); /// collectEnabledRoles() must be called with the `mutex` locked.
}

}
