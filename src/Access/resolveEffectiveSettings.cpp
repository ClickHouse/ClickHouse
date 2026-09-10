#include <Access/resolveEffectiveSettings.h>

#include <Access/AccessControl.h>
#include <Access/SettingsConstraints.h>
#include <Access/User.h>
#include <Access/resolveSetting.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <boost/container/flat_set.hpp>

#include <map>
#include <set>


namespace DB
{
namespace ErrorCodes
{
    extern const int READONLY;
}

void substituteProfiles(
    SettingsProfileElements & elements,
    const std::function<SettingsProfilePtr(const UUID &)> & get_profile_function,
    std::vector<UUID> & profiles,
    std::vector<UUID> & substituted_profiles,
    std::unordered_map<UUID, String> & names_of_substituted_profiles)
{
    profiles = elements.toProfileIDs();

    boost::container::flat_set<UUID> substituted_profiles_set;
    size_t i = elements.size();
    while (i != 0)
    {
        auto & element = elements[--i];
        if (!element.parent_profile)
            continue;

        auto profile_id = *element.parent_profile;
        element.parent_profile.reset();
        if (substituted_profiles_set.count(profile_id))
            continue;

        auto profile = get_profile_function(profile_id);
        if (!profile)
            continue;

        const auto & profile_elements = profile->elements;
        elements.insert(elements.begin() + i, profile_elements.begin(), profile_elements.end());
        i += profile_elements.size();
        substituted_profiles.push_back(profile_id);
        substituted_profiles_set.insert(profile_id);
        names_of_substituted_profiles.emplace(profile_id, profile->getName());
    }
    std::reverse(substituted_profiles.begin(), substituted_profiles.end());

    std::erase_if(profiles, [&substituted_profiles_set](const UUID & profile_id)
    {
        return !substituted_profiles_set.contains(profile_id);
    });
}

ResolvedSettingsProfileElements resolveSettingsProfileElements(
    const std::optional<UUID> & default_profile_id,
    const SettingsProfilesByID & all_profiles,
    const UUID & user_id,
    const boost::container::flat_set<UUID> & enabled_roles,
    const SettingsProfileElements & settings_from_enabled_roles,
    const SettingsProfileElements & settings_from_user)
{
    ResolvedSettingsProfileElements result;
    if (default_profile_id)
        result.elements.emplace_back().parent_profile = *default_profile_id;

    for (const auto & [profile_id, profile] : all_profiles)
    {
        if (profile->to_roles.match(user_id, enabled_roles))
            result.elements.emplace_back().parent_profile = profile_id;
    }

    result.elements.merge(settings_from_enabled_roles, /* normalize= */ false);
    result.elements.merge(settings_from_user, /* normalize= */ false);

    auto get_profile = [&all_profiles](const UUID & id) -> SettingsProfilePtr
    {
        auto it = all_profiles.find(id);
        return it == all_profiles.end() ? nullptr : it->second;
    };
    substituteProfiles(result.elements, get_profile, result.profiles, result.substituted_profiles, result.names_of_substituted_profiles);
    return result;
}


namespace
{
    /// What a setting ends up being for a user: its value and its constraints, after every profile,
    /// role and override that reaches that user has been applied. `setting_name` stays empty: the name
    /// is the key this is stored under.
    using ResolvedSettings = std::map<String, SettingsProfileElement>;

    /// Folds a list of profile elements into one element per setting: within a list the last occurrence
    /// of a field wins, and the two names of a `MergeTree` setting are one setting.
    ResolvedSettings foldElements(const SettingsProfileElements & elements)
    {
        ResolvedSettings result;
        for (const auto & element : elements)
        {
            if (element.setting_name.empty() || SettingsProfileElements::isAllowBackupSetting(element.setting_name))
                continue;

            auto & folded = result[resolveSettingName(element.setting_name)];
            if (element.value)
                folded.value = element.value;
            if (element.min_value)
                folded.min_value = element.min_value;
            if (element.max_value)
                folded.max_value = element.max_value;
            if (!element.disallowed_values.empty())
                folded.disallowed_values.insert(
                    folded.disallowed_values.end(), element.disallowed_values.begin(), element.disallowed_values.end());
            if (element.isConstraint())
                folded.writability = element.writability.value_or(SettingConstraintWritability::WRITABLE);
        }

        for (auto & item : result)
        {
            auto & element = item.second;
            if (element.writability == SettingConstraintWritability::WRITABLE && !element.min_value && !element.max_value
                && element.disallowed_values.empty())
                element.writability.reset();
        }
        return result;
    }

    /// The settings an entity carries itself, or nothing for an entity that carries none.
    const SettingsProfileElements * ownSettings(const AccessEntityPtr & entity)
    {
        if (const auto * user = typeid_cast<const User *>(entity.get()))
            return &user->settings;
        if (const auto * role = typeid_cast<const Role *>(entity.get()))
            return &role->settings;
        if (const auto * profile = typeid_cast<const SettingsProfile *>(entity.get()))
            return &profile->elements;
        return nullptr;
    }

    /// A self-contained access graph. It is read once, then copied and changed in memory to obtain the
    /// post-mutation graph, so validation compares two states from the same snapshot.
    class AccessGraph
    {
    public:
        explicit AccessGraph(const AccessControl & access_control)
            : default_profile_id(access_control.getDefaultProfileId())
        {
            for (auto && [id, user] : access_control.readAllWithIDs<User>())
                users.emplace(id, std::move(user));
            for (auto && [id, role] : access_control.readAllWithIDs<Role>())
                roles.emplace(id, std::move(role));
            for (auto && [id, profile] : access_control.readAllWithIDs<SettingsProfile>())
                profiles.emplace(id, std::move(profile));
        }

        void apply(const PendingAccessEntities & pending)
        {
            for (const auto & [id, entity] : pending)
            {
                users.erase(id);
                roles.erase(id);
                profiles.erase(id);

                if (auto user = typeid_cast<UserPtr>(entity))
                    users.emplace(id, std::move(user));
                else if (auto role = typeid_cast<RolePtr>(entity))
                    roles.emplace(id, std::move(role));
                else if (auto profile = typeid_cast<SettingsProfilePtr>(entity))
                    profiles.emplace(id, std::move(profile));
            }
        }

        AccessEntityPtr get(const UUID & id) const
        {
            if (auto it = users.find(id); it != users.end())
                return it->second;
            if (auto it = roles.find(id); it != roles.end())
                return it->second;
            if (auto it = profiles.find(id); it != profiles.end())
                return it->second;
            return nullptr;
        }

        UserPtr getUser(const UUID & id) const
        {
            auto it = users.find(id);
            return it == users.end() ? nullptr : it->second;
        }

        const std::unordered_map<UUID, UserPtr> & allUsers() const { return users; }

        ResolvedSettings resolveForUser(const UUID & user_id, const User & user) const
        {
            EnabledRolesInfo roles_info;
            boost::container::flat_set<UUID> skip_ids;
            auto get_role = [this](const UUID & id) -> RolePtr
            {
                auto it = roles.find(id);
                return it == roles.end() ? nullptr : it->second;
            };

            collectRoles(
                roles_info,
                skip_ids,
                get_role,
                user.granted_roles.findGranted(user.default_roles),
                user.granted_roles.findGrantedWithAdminOption(user.default_roles),
                /* settings_only= */ true);

            return foldElements(
                resolveSettingsProfileElements(
                    default_profile_id, profiles, user_id, roles_info.enabled_roles, roles_info.settings_from_enabled_roles, user.settings)
                    .elements);
        }

        /// What a user of this name would resolve to if it carried nothing of its own. This is the state a
        /// user that the statement creates is compared against, so that a setting the server already puts
        /// in effect for everybody is not read as something the statement introduced.
        ResolvedSettings resolveForNewUser(const UUID & user_id, const String & user_name) const
        {
            User blank;
            blank.setName(user_name);
            blank.default_roles.clear();
            return resolveForUser(user_id, blank);
        }

    private:
        std::unordered_map<UUID, UserPtr> users;
        std::unordered_map<UUID, RolePtr> roles;
        SettingsProfilesByID profiles;
        std::optional<UUID> default_profile_id;
    };

    std::unordered_map<UUID, UUID> findReplacements(const AccessGraph & before, const PendingAccessEntities & pending)
    {
        std::unordered_map<UUID, UUID> replacements;
        for (const auto & [new_id, new_entity] : pending)
        {
            if (!new_entity || before.get(new_id))
                continue;

            for (const auto & [old_id, removed_entity] : pending)
            {
                if (removed_entity)
                    continue;
                auto old_entity = before.get(old_id);
                if (old_entity && old_entity->getType() == new_entity->getType() && old_entity->getName() == new_entity->getName())
                {
                    replacements.emplace(new_id, old_id);
                    break;
                }
            }
        }
        return replacements;
    }

    void checkResolvedSettings(const AccessControl & access_control, const ResolvedSettings & before, const ResolvedSettings & after)
    {
        std::set<String> names;
        for (const auto & item : before)
            names.emplace(item.first);
        for (const auto & item : after)
            names.emplace(item.first);

        for (const auto & setting_name : names)
        {
            auto reason = getFeatureTierRestriction(access_control, setting_name, settingGetTier(setting_name));
            if (!reason)
                continue;

            SettingsProfileElement before_element;
            if (auto it = before.find(setting_name); it != before.end())
                before_element = it->second;
            SettingsProfileElement after_element;
            if (auto it = after.find(setting_name); it != after.end())
                after_element = it->second;

            if (!before_element.value)
                before_element.value = settingDefaultValue(setting_name);
            if (!after_element.value)
                after_element.value = settingDefaultValue(setting_name);

            if (before_element != after_element)
                throw Exception(*reason, ErrorCodes::READONLY);
        }
    }

    /// Whether writing `new_entity` over `old_entity` can change the settings some user resolves to.
    /// Everything an access statement does that has nothing to do with settings - granting a privilege,
    /// renaming, changing authentication - is answered here, without resolving the graph.
    bool mayChangeSettingsInEffect(const AccessEntityPtr & old_entity, const AccessEntityPtr & new_entity)
    {
        const auto * old_user = typeid_cast<const User *>(old_entity.get());
        const auto * new_user = typeid_cast<const User *>(new_entity.get());
        if (old_user && new_user)
        {
            return old_user->settings != new_user->settings || old_user->granted_roles != new_user->granted_roles
                || old_user->default_roles != new_user->default_roles;
        }
        /// Dropping a user constrains nothing: no other user's settings depend on it.
        if (old_user && !new_entity)
            return false;

        const auto * old_role = typeid_cast<const Role *>(old_entity.get());
        const auto * new_role = typeid_cast<const Role *>(new_entity.get());
        if (old_role && new_role)
            return old_role->settings != new_role->settings || old_role->granted_roles != new_role->granted_roles;

        const auto * old_profile = typeid_cast<const SettingsProfile *>(old_entity.get());
        const auto * new_profile = typeid_cast<const SettingsProfile *>(new_entity.get());
        if (old_profile && new_profile)
            return old_profile->elements != new_profile->elements || old_profile->to_roles != new_profile->to_roles;

        auto is_relevant_type = [](const AccessEntityPtr & entity)
        {
            if (!entity)
                return false;
            auto type = entity->getType();
            return type == AccessEntityType::USER || type == AccessEntityType::ROLE
                || type == AccessEntityType::SETTINGS_PROFILE;
        };
        return is_relevant_type(old_entity) || is_relevant_type(new_entity);
    }
}


void checkFeatureTierForPendingAccessEntities(const AccessControl & access_control, const PendingAccessEntities & pending)
{
    if (!isAnyFeatureTierRestricted(access_control))
        return;

    bool relevant = false;
    for (const auto & [id, entity] : pending)
    {
        if (mayChangeSettingsInEffect(access_control.tryRead(id), entity))
        {
            relevant = true;
            break;
        }
    }
    if (!relevant)
        return;

    auto refuse_if_restricted = [&](const String & setting_name)
    {
        if (auto reason = getFeatureTierRestriction(access_control, setting_name, settingGetTier(setting_name)))
            throw Exception(*reason, ErrorCodes::READONLY);
    };

    AccessGraph before(access_control);
    auto replacements = findReplacements(before, pending);

    /// Writing a setting of a disabled tier into an entity is refused even when no user resolves to that
    /// entity yet, because the entity is what a later `GRANT` or `TO` clause would put in effect. Rewriting
    /// the value the entity already holds changes nothing and is allowed.
    for (const auto & [id, new_entity] : pending)
    {
        const auto * new_elements = ownSettings(new_entity);
        if (!new_elements)
            continue;

        auto old_entity = before.get(id);
        if (!old_entity)
        {
            if (auto it = replacements.find(id); it != replacements.end())
                old_entity = before.get(it->second);
        }
        const auto * old_elements = ownSettings(old_entity);
        auto old_folded = old_elements ? foldElements(*old_elements) : ResolvedSettings{};
        for (const auto & [setting_name, element] : foldElements(*new_elements))
        {
            auto it = old_folded.find(setting_name);
            if (it != old_folded.end() && it->second == element)
                continue;
            refuse_if_restricted(setting_name);
        }
    }

    AccessGraph after = before;
    after.apply(pending);

    auto check_user = [&](const UUID & user_id, const UserPtr & user)
    {
        auto after_settings = after.resolveForUser(user_id, *user);

        auto old_id = user_id;
        if (auto it = replacements.find(user_id); it != replacements.end())
            old_id = it->second;
        auto old_user = before.getUser(old_id);
        auto before_settings = old_user ? before.resolveForUser(old_id, *old_user) : before.resolveForNewUser(user_id, user->getName());

        checkResolvedSettings(access_control, before_settings, after_settings);
    };

    bool changes_only_users = true;
    for (const auto & [id, new_entity] : pending)
    {
        auto old_entity = before.get(id);
        if ((old_entity && old_entity->getType() != AccessEntityType::USER)
            || (new_entity && new_entity->getType() != AccessEntityType::USER))
        {
            changes_only_users = false;
            break;
        }
    }

    if (changes_only_users)
    {
        for (const auto & [id, entity] : pending)
        {
            if (auto user = after.getUser(id))
                check_user(id, user);
        }
    }
    else
    {
        for (const auto & [id, user] : after.allUsers())
            check_user(id, user);
    }
}

}
