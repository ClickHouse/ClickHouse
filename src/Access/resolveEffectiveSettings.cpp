#include <Access/resolveEffectiveSettings.h>

#include <Access/AccessControl.h>
#include <Access/SettingsConstraints.h>
#include <Access/User.h>
#include <Access/resolveSetting.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <boost/container/flat_set.hpp>

#include <map>


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
                folded.disallowed_values = element.disallowed_values;
            if (element.writability)
                folded.writability = element.writability;
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

    /// The access graph as it is stored, with the entities a statement is about to write applied on top.
    class AccessGraph
    {
    public:
        AccessGraph(const AccessControl & access_control_, const PendingAccessEntities & pending_)
            : access_control(access_control_), pending(pending_)
        {
            for (const auto & id : access_control.findAll<SettingsProfile>())
                if (auto profile = get<SettingsProfile>(id))
                    all_profiles.emplace(id, profile);

            for (const auto & [id, entity] : pending)
            {
                if (auto profile = typeid_cast<SettingsProfilePtr>(entity))
                    all_profiles.insert_or_assign(id, profile);
                else if (all_profiles.contains(id))
                    all_profiles.erase(id);
            }

            default_profile_id = access_control.getDefaultProfileId();
        }

        /// The users the graph holds, by name. A user is looked up by name and not by id because
        /// `CREATE USER OR REPLACE` writes a new id for a user that keeps its name.
        std::map<String, std::pair<UUID, UserPtr>> allUsers() const
        {
            std::map<String, std::pair<UUID, UserPtr>> result;
            for (const auto & id : access_control.findAll<User>())
                if (auto user = get<User>(id))
                    result.insert_or_assign(user->getName(), std::pair{id, user});

            for (const auto & [id, entity] : pending)
            {
                std::erase_if(result, [&](const auto & item) { return item.second.first == id; });
                if (auto user = typeid_cast<UserPtr>(entity))
                    result.insert_or_assign(user->getName(), std::pair{id, user});
            }
            return result;
        }

        ResolvedSettings resolveForUser(const UUID & user_id, const User & user) const
        {
            EnabledRolesInfo roles_info;
            boost::container::flat_set<UUID> skip_ids;
            auto get_role = [this](const UUID & id) { return getRole(id); };

            for (const auto & role_id : user.granted_roles.findGranted(user.default_roles))
                collectRoles(roles_info, skip_ids, get_role, role_id, true, false);
            for (const auto & role_id : user.granted_roles.findGrantedWithAdminOption(user.default_roles))
                collectRoles(roles_info, skip_ids, get_role, role_id, true, true);

            SettingsProfileElements merged_settings;
            if (default_profile_id)
                merged_settings.emplace_back().parent_profile = *default_profile_id;

            for (const auto & [profile_id, profile] : all_profiles)
                if (profile->to_roles.match(user_id, roles_info.enabled_roles))
                    merged_settings.emplace_back().parent_profile = profile_id;

            merged_settings.merge(roles_info.settings_from_enabled_roles, /* normalize= */ false);
            merged_settings.merge(user.settings, /* normalize= */ false);

            std::vector<UUID> profiles;
            std::vector<UUID> substituted_profiles;
            std::unordered_map<UUID, String> names_of_profiles;
            auto get_profile = [this](const UUID & id) -> SettingsProfilePtr
            {
                auto it = all_profiles.find(id);
                return it == all_profiles.end() ? nullptr : it->second;
            };
            substituteProfiles(merged_settings, get_profile, profiles, substituted_profiles, names_of_profiles);

            return foldElements(merged_settings);
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
        template <typename EntityClassT>
        std::shared_ptr<const EntityClassT> get(const UUID & id) const
        {
            auto it = pending.find(id);
            if (it != pending.end())
                return typeid_cast<std::shared_ptr<const EntityClassT>>(it->second);
            return access_control.tryRead<EntityClassT>(id);
        }

        /// The same roles are read again for every user, so read each of them once.
        RolePtr getRole(const UUID & id) const
        {
            auto it = roles.find(id);
            if (it == roles.end())
                it = roles.emplace(id, get<Role>(id)).first;
            return it->second;
        }

        const AccessControl & access_control;
        const PendingAccessEntities & pending;
        /// Ordered, so that the merge order of the profiles assigned to a user is the same before and after.
        std::map<UUID, SettingsProfilePtr> all_profiles;
        mutable std::unordered_map<UUID, RolePtr> roles;
        std::optional<UUID> default_profile_id;
    };

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

    /// Writing a setting of a disabled tier into an entity is refused even when no user resolves to that
    /// entity yet, because the entity is what a later `GRANT` or `TO` clause would put in effect. Rewriting
    /// the value the entity already holds changes nothing and is allowed.
    for (const auto & pending_entry : pending)
    {
        const auto & new_entity = pending_entry.second;
        const auto * new_elements = ownSettings(new_entity);
        if (!new_elements)
            continue;

        /// By name, not by `id`: `CREATE ... OR REPLACE` writes a new id for the same entity.
        auto old_id = access_control.find(new_entity->getType(), new_entity->getName());
        const auto * old_elements = old_id ? ownSettings(access_control.tryRead(*old_id)) : nullptr;
        auto old_folded = old_elements ? foldElements(*old_elements) : ResolvedSettings{};
        for (const auto & [setting_name, element] : foldElements(*new_elements))
        {
            auto it = old_folded.find(setting_name);
            if (it != old_folded.end() && it->second == element)
                continue;
            refuse_if_restricted(setting_name);
        }
    }

    static const PendingAccessEntities nothing_pending;
    AccessGraph before(access_control, nothing_pending);
    AccessGraph after(access_control, pending);

    auto users_before = before.allUsers();

    for (const auto & [user_name, id_and_user] : after.allUsers())
    {
        const auto & [user_id, user] = id_and_user;
        auto after_settings = after.resolveForUser(user_id, *user);

        auto it = users_before.find(user_name);
        auto before_settings = (it == users_before.end())
            ? before.resolveForNewUser(user_id, user_name)
            : before.resolveForUser(it->second.first, *it->second.second);

        for (const auto & [setting_name, after_element] : after_settings)
        {
            auto before_it = before_settings.find(setting_name);
            if (before_it != before_settings.end() && before_it->second == after_element)
                continue;
            refuse_if_restricted(setting_name);
        }

        /// A setting the statement stops putting in effect is a change of the value in effect too.
        for (const auto & [setting_name, before_element] : before_settings)
        {
            if (after_settings.contains(setting_name))
                continue;
            refuse_if_restricted(setting_name);
        }
    }
}

}
