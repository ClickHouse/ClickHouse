#include <Access/RoleCache.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/resolveEffectiveSettings.h>
#include <Access/AccessControl.h>
#include <boost/container/flat_set.hpp>
#include <base/FnTraits.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event RoleCacheRecalculations;
    extern const Event RoleCacheRecalculationMicroseconds;
}

namespace DB
{
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
    collectEnabledRoles(*res, [this](const UUID & id) TSA_NO_THREAD_SAFETY_ANALYSIS { return getRole(id); }, referenced_roles, nullptr);
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
            /// Declared first to send notifications after `mutex` is unlocked.
            scope_guard notifications;

            {
                std::lock_guard lock{mutex};
                for (const auto & change : changes)
                {
                    if (auto changed_role = change.entity ? typeid_cast<RolePtr>(change.entity) : nullptr)
                        roleChanged(change.id, changed_role);
                    else
                        roleRemoved(change.id);
                }
            }
            collectEnabledRolesIfNeeded(&notifications);
        });

    /// Set after the subscription is established.
    subscribed = true;
}


void RoleCache::collectEnabledRolesIfNeeded(scope_guard * notifications)
{
    /// Runs without `mutex`: it is called only from sendNotifications(), one call at a time,
    /// so nothing else updates the existing sets or `need_collect_enabled_roles` meanwhile.
    VectorWithMemoryTracking<std::shared_ptr<EnabledRoles>> enabled_sets;
    {
        std::lock_guard lock{mutex};
        if (!need_collect_enabled_roles)
            return;
        for (auto i = enabled_roles_by_params.begin(), e = enabled_roles_by_params.end(); i != e;)
        {
            if (auto enabled_roles = i->second.lock())
            {
                enabled_sets.push_back(std::move(enabled_roles));
                ++i;
            }
            else
                i = enabled_roles_by_params.erase(i);
        }
        /// Rebuilt from scratch below, so roles of expired sets drop out; getEnabledRoles() adds those of the sets it creates meanwhile.
        referenced_roles.clear();
    }

    ProfileEvents::increment(ProfileEvents::RoleCacheRecalculations);
    Stopwatch watch;

    auto get_role_function = [this](const UUID & id)
    {
        std::lock_guard lock{mutex};
        return getRole(id);
    };

    UnorderedSetWithMemoryTracking<UUID> new_referenced_roles;
    for (const auto & enabled_roles : enabled_sets)
        collectEnabledRoles(*enabled_roles, get_role_function, new_referenced_roles, notifications);

    {
        std::lock_guard lock{mutex};
        referenced_roles.insert(new_referenced_roles.begin(), new_referenced_roles.end());
        /// Cleared only after a successful rebuild, so a throwing one is retried with the next batch.
        need_collect_enabled_roles = false;
    }

    const auto elapsed_ms = watch.elapsedMilliseconds();
    ProfileEvents::increment(ProfileEvents::RoleCacheRecalculationMicroseconds, watch.elapsedMicroseconds());
    if (elapsed_ms >= 1000)
        LOG_DEBUG(getLogger("RoleCache"), "Recalculated enabled roles for {} enabled set(s) in {} ms", enabled_sets.size(), elapsed_ms);
    else
        LOG_TRACE(getLogger("RoleCache"), "Recalculated enabled roles for {} enabled set(s) in {} ms", enabled_sets.size(), elapsed_ms);
}


void RoleCache::collectEnabledRoles(
    EnabledRoles & enabled_roles,
    const std::function<RolePtr(const UUID &)> & get_role_function,
    UnorderedSetWithMemoryTracking<UUID> & referenced,
    scope_guard * notifications)
{
    /// Collect enabled roles. That includes the current roles, the roles granted to the current roles, and so on.
    auto new_info = std::make_shared<EnabledRolesInfo>();
    UnorderedSetWithMemoryTracking<UUID> skip_ids;

    for (const auto & role_id : enabled_roles.params.current_roles)
        collectRoles(*new_info, skip_ids, get_role_function, role_id, true, false);
    for (const auto & role_id : enabled_roles.params.current_roles_with_admin_option)
        collectRoles(*new_info, skip_ids, get_role_function, role_id, true, true);

    /// Remember which roles take part in this enabled set, so that a later change to one of them triggers
    /// a recalculation (and a change to a role nobody uses does not).
    /// We need to remember `skip_ids` too in order to trigger a recalculation if they appear later
    /// (for example if a role with a granted role is replicated before that granted role);
    referenced.insert(new_info->enabled_roles.begin(), new_info->enabled_roles.end());
    referenced.insert(skip_ids.begin(), skip_ids.end());

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
