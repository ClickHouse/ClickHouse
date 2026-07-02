#include <Access/RoleCache.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/AccessControl.h>
#include <boost/container/flat_set.hpp>
#include <base/FnTraits.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event RoleCacheRecalculations;
    extern const Event RoleCacheRecalculationMicroseconds;
}

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
        roles_info.settings_from_enabled_roles.merge(role->settings, /* normalize= */ false);

        for (const auto & granted_role : role->granted_roles.getGranted())
            collectRoles(roles_info, skip_ids, get_role_function, granted_role, false, false);

        for (const auto & granted_role : role->granted_roles.getGrantedWithAdminOption())
            collectRoles(roles_info, skip_ids, get_role_function, granted_role, false, true);
    }
}


RoleCache::RoleCache(const AccessControl & access_control_, int expiration_time_seconds)
    : access_control(access_control_), cache(expiration_time_seconds * 1000ll /* 10 minutes by default*/)
{
}


RoleCache::~RoleCache() = default;


std::shared_ptr<const EnabledRoles>
RoleCache::getEnabledRoles(const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option)
{
    auto role_set = boost::container::flat_set<UUID>(roles.begin(), roles.end());
    auto role_with_admin_option_set = boost::container::flat_set<UUID>(roles_with_admin_option.begin(), roles_with_admin_option.end());

    return getEnabledRoles(std::move(role_set), std::move(role_with_admin_option_set));
}


std::shared_ptr<const EnabledRoles>
RoleCache::getEnabledRoles(boost::container::flat_set<UUID> roles, boost::container::flat_set<UUID> roles_with_admin_option)
{
    std::lock_guard lock{mutex};
    ensureSubscribed();

    EnabledRoles::Params params;
    params.current_roles = std::move(roles);
    params.current_roles_with_admin_option = std::move(roles_with_admin_option);
    auto it = enabled_roles_by_params.find(params);
    if (it != enabled_roles_by_params.end())
    {
        if (auto enabled_roles = it->second.lock())
            return enabled_roles;
        enabled_roles_by_params.erase(it);
    }

    auto res = std::shared_ptr<EnabledRoles>(new EnabledRoles(params));
    collectEnabledRoles(*res, nullptr);
    enabled_roles_by_params.emplace(std::move(params), res);
    return res;
}


void RoleCache::ensureSubscribed()
{
    /// `mutex` is already locked.

    /// Lazy (not in the ctor): `changes_notifier` is constructed after the caches in `AccessControl`.
    if (subscribed)
        return;

    /// A single subscription for all roles: each batch updates the cached roles and runs one coalesced
    /// recalculation, instead of one subscription (and one recalculation) per role.
    subscription = access_control.subscribeForChanges<Role>(
        [this](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            /// Declared before `lock` to send notifications after the mutex will be unlocked.
            scope_guard notifications;

            std::lock_guard lock{mutex};
            for (const auto & change : changes)
            {
                if (auto changed_role = change.entity ? typeid_cast<RolePtr>(change.entity) : nullptr)
                    roleChanged(change.id, changed_role);
                else
                    roleRemoved(change.id);
            }
            collectEnabledRolesIfNeeded(&notifications);
        });

    /// Set after the subscription is established.
    subscribed = true;
}


void RoleCache::collectEnabledRolesIfNeeded(scope_guard * notifications)
{
    /// `mutex` is already locked.
    if (!need_collect_enabled_roles)
        return;
    /// Clear the flag only after a successful rebuild, so a throwing recompute is retried next batch.
    collectEnabledRoles(notifications);
    need_collect_enabled_roles = false;
}


void RoleCache::collectEnabledRoles(scope_guard * notifications)
{
    /// `mutex` is already locked.

    ProfileEvents::increment(ProfileEvents::RoleCacheRecalculations);
    Stopwatch watch;
    /// Recompute the set of roles that take part in some enabled set from scratch (it also drops roles
    /// that are only referenced by enabled sets that have expired by now).
    referenced_roles.clear();
    for (auto i = enabled_roles_by_params.begin(), e = enabled_roles_by_params.end(); i != e;)
    {
        if (auto enabled_roles = i->second.lock())
        {
            collectEnabledRoles(*enabled_roles, notifications);
            ++i;
        }
        else
        {
            i = enabled_roles_by_params.erase(i);
        }
    }

    const auto elapsed_ms = watch.elapsedMilliseconds();
    ProfileEvents::increment(ProfileEvents::RoleCacheRecalculationMicroseconds, watch.elapsedMicroseconds());
    /// O(enabled sets * roles), under `mutex` that the ContextAccess build path also takes.
    if (elapsed_ms >= 1000)
        LOG_WARNING(getLogger("RoleCache"), "Recalculated enabled roles for {} enabled set(s) in {} ms", enabled_roles_by_params.size(), elapsed_ms);
    else
        LOG_DEBUG(getLogger("RoleCache"), "Recalculated enabled roles for {} enabled set(s) in {} ms", enabled_roles_by_params.size(), elapsed_ms);
}


void RoleCache::collectEnabledRoles(EnabledRoles & enabled_roles, scope_guard * notifications)
{
    /// `mutex` is already locked.

    /// Collect enabled roles. That includes the current roles, the roles granted to the current roles, and so on.
    auto new_info = std::make_shared<EnabledRolesInfo>();
    boost::container::flat_set<UUID> skip_ids;

    auto get_role_function = [this](const UUID & id) TSA_NO_THREAD_SAFETY_ANALYSIS { return getRole(id); };

    for (const auto & current_role : enabled_roles.params.current_roles)
        collectRoles(*new_info, skip_ids, get_role_function, current_role, true, false);

    for (const auto & current_role : enabled_roles.params.current_roles_with_admin_option)
        collectRoles(*new_info, skip_ids, get_role_function, current_role, true, true);

    /// Remember which roles take part in this enabled set, so that a later change to one of them triggers
    /// a recalculation (and a change to a role nobody uses does not).
    /// We need to remember `skip_ids` too in order to trigger a recalculation if they appear later
    /// (for example if a role with a granted role is replicated before that granted role);
    referenced_roles.insert(new_info->enabled_roles.begin(), new_info->enabled_roles.end());
    referenced_roles.insert(skip_ids.begin(), skip_ids.end());

    /// Collect data from the collected roles.
    enabled_roles.setRolesInfo(new_info, notifications);
}


RolePtr RoleCache::getRole(const UUID & role_id)
{
    /// `mutex` is already locked.

    auto role_from_cache = cache.get(role_id);
    if (role_from_cache)
        return *role_from_cache;

    auto role = access_control.tryRead<Role>(role_id);
    if (role)
    {
        cache.add(role_id, role);
        return role;
    }

    return nullptr;
}


void RoleCache::roleChanged(const UUID & role_id, const RolePtr & changed_role)
{
    /// `mutex` is already locked.

    auto role_from_cache = cache.get(role_id);
    if (role_from_cache)
    {
        /// We update the role stored in a cache entry only if that entry has not expired yet.
        *role_from_cache = changed_role;
        cache.update(role_id, role_from_cache);
    }

    /// Recalculate the access rights only if this role takes part in some enabled set.
    if (referenced_roles.contains(role_id))
        need_collect_enabled_roles = true;
}


void RoleCache::roleRemoved(const UUID & role_id)
{
    /// `mutex` is already locked.

    /// If a cache entry with the role has expired already, that remove() will do nothing.
    cache.remove(role_id);

    /// Recalculate the access rights only if this role takes part in some enabled set.
    if (referenced_roles.contains(role_id))
        need_collect_enabled_roles = true;
}

}
