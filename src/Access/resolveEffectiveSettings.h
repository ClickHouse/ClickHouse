#pragma once

#include <Access/EnabledRolesInfo.h>
#include <Access/IAccessEntity.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsProfileElement.h>
#include <base/FnTraits.h>
#include <boost/container/flat_set.hpp>

#include <functional>
#include <map>
#include <unordered_map>


namespace DB
{
class AccessControl;

/// Collects `role_id` and, transitively, every role granted to it into `roles_info`.
/// `get_role_function` returns nullptr for a role that cannot be read; such ids land in `skip_ids`
/// so that each of them is looked up once.
void collectRoles(
    EnabledRolesInfo & roles_info,
    boost::container::flat_set<UUID> & skip_ids,
    Fn<RolePtr(const UUID &)> auto && get_role_function,
    const UUID & role_id,
    bool is_current_role,
    bool with_admin_option,
    bool settings_only = false)
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

    if (!settings_only)
    {
        roles_info.names_of_roles[role_id] = role->getName();
        roles_info.access.makeUnion(role->access);
    }
    roles_info.settings_from_enabled_roles.merge(role->settings, /* normalize= */ false);

    for (const auto & granted_role : role->granted_roles.getGranted())
        collectRoles(roles_info, skip_ids, get_role_function, granted_role, false, false, settings_only);

    for (const auto & granted_role : role->granted_roles.getGrantedWithAdminOption())
        collectRoles(roles_info, skip_ids, get_role_function, granted_role, false, true, settings_only);
}

/// Collects two sets of current roles and their transitive grants.
void collectRoles(
    EnabledRolesInfo & roles_info,
    boost::container::flat_set<UUID> & skip_ids,
    Fn<RolePtr(const UUID &)> auto && get_role_function,
    const auto & current_roles,
    const auto & current_roles_with_admin_option,
    bool settings_only = false)
{
    for (const auto & role_id : current_roles)
        collectRoles(roles_info, skip_ids, get_role_function, role_id, true, false, settings_only);
    for (const auto & role_id : current_roles_with_admin_option)
        collectRoles(roles_info, skip_ids, get_role_function, role_id, true, true, settings_only);
}

/// Replaces every element referencing a parent profile with the elements of that profile, in place.
/// The substitution runs in reverse order because the same profile can occur several times, and then
/// the last occurrence overrides the previous ones.
void substituteProfiles(
    SettingsProfileElements & elements,
    const std::function<SettingsProfilePtr(const UUID &)> & get_profile_function,
    std::vector<UUID> & profiles,
    std::vector<UUID> & substituted_profiles,
    std::unordered_map<UUID, String> & names_of_substituted_profiles);

using SettingsProfilesByID = std::map<UUID, SettingsProfilePtr>;

struct ResolvedSettingsProfileElements
{
    SettingsProfileElements elements;
    std::vector<UUID> profiles;
    std::vector<UUID> substituted_profiles;
    std::unordered_map<UUID, String> names_of_substituted_profiles;
};

/// Resolves the complete settings-profile chain for one user. Both the live cache and pending-write
/// validation use this function, so they cannot diverge in profile matching or precedence.
ResolvedSettingsProfileElements resolveSettingsProfileElements(
    const std::optional<UUID> & default_profile_id,
    const SettingsProfilesByID & all_profiles,
    const UUID & user_id,
    const boost::container::flat_set<UUID> & enabled_roles,
    const SettingsProfileElements & settings_from_enabled_roles,
    const SettingsProfileElements & settings_from_user);

/// The access entities a statement is about to write, by id. A null entity means a removal.
using PendingAccessEntities = std::unordered_map<UUID, AccessEntityPtr>;

/// Refuses the pending write if it moves, for any user, the value in effect of a setting whose tier
/// `allow_feature_tier` disables.
///
/// The decision is the difference between the settings every user resolves to before the write and
/// after it, not the statement. That is what makes `GRANT`, `DROP ROLE`, a settings profile's `TO`
/// clause and an override dropped by omission one case instead of many: none of them names a setting,
/// and all of them change which settings are in effect.
void checkFeatureTierForPendingAccessEntities(const AccessControl & access_control, const PendingAccessEntities & pending);

}
