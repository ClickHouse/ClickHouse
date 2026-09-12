#pragma once

#include <Access/EnabledRolesInfo.h>
#include <Access/IAccessEntity.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsProfileElement.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
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
    UnorderedSetWithMemoryTracking<UUID> & skip_ids,
    const std::function<RolePtr(const UUID &)> & get_role_function,
    const UUID & role_id,
    bool is_current_role,
    bool with_admin_option,
    bool settings_only = false);

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
using PendingAccessEntities = UnorderedMapWithMemoryTracking<UUID, AccessEntityPtr>;
using FeatureTierAccessEntityChecker = std::function<void(const PendingAccessEntities & pending, const PendingAccessEntities & current)>;

/// Prepares an immutable graph snapshot for checks which must run from inside an access storage's
/// update callback. An empty result means the pending change cannot affect settings in effect.
FeatureTierAccessEntityChecker prepareFeatureTierAccessEntityChecker(
    const AccessControl & access_control,
    const PendingAccessEntities & pending,
    const PendingAccessEntities & current = {},
    bool force = false);

/// Refuses the pending write if it changes a setting whose tier `allow_feature_tier` disables for a
/// user, or for a role or settings profile which could later carry that setting to a user.
///
/// The decision is the difference between the settings every user resolves to before the write and
/// after it, not the statement. That is what makes `GRANT`, `DROP ROLE`, a settings profile's `TO`
/// clause and an override dropped by omission one case instead of many: none of them names a setting,
/// and all of them change which settings are in effect.
void checkFeatureTierForPendingAccessEntities(
    const AccessControl & access_control, const PendingAccessEntities & pending, const PendingAccessEntities & current = {});
}
