#include <Access/resolveEffectiveSettings.h>

#include <Access/AccessControl.h>
#include <Access/SettingsConstraints.h>
#include <Access/User.h>
#include <Access/resolveSetting.h>
#include <Common/Exception.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
    extern const int READONLY;
}

void collectRoles(
    EnabledRolesInfo & roles_info,
    UnorderedSetWithMemoryTracking<UUID> & skip_ids,
    const std::function<RolePtr(const UUID &)> & get_role_function,
    const UUID & role_id,
    bool is_current_role,
    bool with_admin_option,
    bool settings_only)
{
    if (roles_info.enabled_roles.contains(role_id))
    {
        if (is_current_role)
            roles_info.current_roles.emplace(role_id);
        if (with_admin_option)
            roles_info.enabled_roles_with_admin_option.emplace(role_id);
        return;
    }

    if (skip_ids.contains(role_id))
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

void substituteProfiles(
    SettingsProfileElements & elements,
    const std::function<SettingsProfilePtr(const UUID &)> & get_profile_function,
    std::vector<UUID> & profiles,
    std::vector<UUID> & substituted_profiles,
    std::unordered_map<UUID, String> & names_of_substituted_profiles)
{
    profiles = elements.toProfileIDs();

    UnorderedSetWithMemoryTracking<UUID> substituted_profiles_set;
    size_t i = elements.size();
    while (i != 0)
    {
        auto & element = elements[--i];
        if (!element.parent_profile)
            continue;

        auto profile_id = *element.parent_profile;
        element.parent_profile.reset();
        if (substituted_profiles_set.contains(profile_id))
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
    using ResolvedSettings = MapWithMemoryTracking<String, SettingsProfileElement>;
    using AccessEntityIDs = UnorderedSetWithMemoryTracking<UUID>;
    using UsersByID = UnorderedMapWithMemoryTracking<UUID, UserPtr>;
    using RolesByID = UnorderedMapWithMemoryTracking<UUID, RolePtr>;
    using Replacements = UnorderedMapWithMemoryTracking<UUID, UUID>;

    /// Folds a list of profile elements into one element per setting, using the same constraint merge
    /// rules as `SettingsConstraints`, and treats the two names of a `MergeTree` setting as one setting.
    ResolvedSettings foldElements(const AccessControl & access_control, const SettingsProfileElements & elements)
    {
        ResolvedSettings result;
        for (const auto & element : elements)
        {
            if (element.setting_name.empty() || SettingsProfileElements::isAllowBackupSetting(element.setting_name))
                continue;

            auto & folded = result[resolveSettingName(element.setting_name)];
            if (element.value)
                folded.value = element.value;

            if (!element.isConstraint())
                continue;

            if (access_control.doesSettingsConstraintsReplacePrevious())
            {
                folded.min_value = element.min_value;
                folded.max_value = element.max_value;
                folded.disallowed_values = element.disallowed_values;
                folded.writability = element.writability.value_or(SettingConstraintWritability::WRITABLE);
            }
            else
            {
                if (element.min_value)
                    folded.min_value = element.min_value;
                if (element.max_value)
                    folded.max_value = element.max_value;
                if (!element.disallowed_values.empty())
                    folded.disallowed_values = element.disallowed_values;
                if (element.writability == SettingConstraintWritability::CONST)
                    folded.writability = SettingConstraintWritability::CONST;
            }
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
        explicit AccessGraph(
            const AccessControl & access_control_,
            bool read_all_users,
            const PendingAccessEntities & pending,
            const PendingAccessEntities & current)
            : access_control(access_control_)
            , default_profile_id(access_control_.getDefaultProfileId())
            , has_all_users(read_all_users)
        {
            if (read_all_users)
            {
                /// The storages return their entities in lookup order, so the first user of a name is the
                /// one a login resolves to and the rest are shadowed.
                for (auto && [id, user] : access_control_.readAllWithIDs<User>())
                {
                    user_order.emplace_back(id);
                    users.emplace(id, std::move(user));
                }
            }
            else
            {
                UnorderedSetWithMemoryTracking<UUID> ids;
                for (const auto & item : pending)
                    ids.emplace(item.first);
                for (const auto & item : current)
                    ids.emplace(item.first);

                for (const auto & id : ids)
                {
                    auto current_it = current.find(id);
                    auto entity = current_it == current.end() ? access_control_.tryRead(id) : current_it->second;
                    if (typeid_cast<const User *>(entity.get()))
                        users.emplace(id, std::static_pointer_cast<const User>(std::move(entity)));
                }
            }
            for (auto && [id, role] : access_control_.readAllWithIDs<Role>())
                roles.emplace(id, std::move(role));
            for (auto && [id, profile] : access_control_.readAllWithIDs<SettingsProfile>())
                profiles.emplace(id, std::move(profile));
        }

        void apply(const PendingAccessEntities & pending, bool new_users_are_shadowed = false)
        {
            for (const auto & [id, entity] : pending)
            {
                bool was_user = users.erase(id);
                roles.erase(id);
                profiles.erase(id);

                if (auto user = typeid_cast<UserPtr>(entity))
                {
                    /// A user this write creates goes ahead of every same-name user, unless the storage it
                    /// lands in is looked up after one which already holds that name.
                    if (!was_user)
                    {
                        if (new_users_are_shadowed)
                            user_order.emplace_back(id);
                        else
                            user_order.insert(user_order.begin(), id);
                    }
                    users.emplace(id, std::move(user));
                }
                else
                {
                    if (was_user)
                        std::erase(user_order, id);
                    if (auto role = typeid_cast<RolePtr>(entity))
                        roles.emplace(id, std::move(role));
                    else if (auto profile = typeid_cast<SettingsProfilePtr>(entity))
                        profiles.emplace(id, std::move(profile));
                }
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

        const UsersByID & allUsers() const { return users; }

        bool hasAllUsers() const { return has_all_users; }

        /// The user a login of this name resolves to, or a null entity if no storage holds that name.
        std::pair<UUID, UserPtr> visibleUser(const String & name) const
        {
            for (const auto & id : user_order)
            {
                auto it = users.find(id);
                if (it != users.end() && it->second->getName() == name)
                    return {id, it->second};
            }
            return {UUIDHelpers::Nil, nullptr};
        }

        ResolvedSettings resolveForUser(const UUID & user_id, const User & user) const
        {
            EnabledRolesInfo roles_info;
            UnorderedSetWithMemoryTracking<UUID> skip_ids;
            auto get_role = [this](const UUID & id) -> RolePtr
            {
                auto it = roles.find(id);
                return it == roles.end() ? nullptr : it->second;
            };

            for (const auto & role_id : user.granted_roles.findGranted(user.default_roles))
                collectRoles(roles_info, skip_ids, get_role, role_id, true, false, /* settings_only= */ true);
            for (const auto & role_id : user.granted_roles.findGrantedWithAdminOption(user.default_roles))
                collectRoles(roles_info, skip_ids, get_role, role_id, true, true, /* settings_only= */ true);

            return foldElements(
                access_control,
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

        ResolvedSettings resolveForRole(const UUID & role_id) const
        {
            EnabledRolesInfo roles_info;
            UnorderedSetWithMemoryTracking<UUID> skip_ids;
            auto get_role = [this](const UUID & id) -> RolePtr
            {
                auto it = roles.find(id);
                return it == roles.end() ? nullptr : it->second;
            };

            collectRoles(roles_info, skip_ids, get_role, role_id, true, false, /* settings_only= */ true);
            return foldElements(
                access_control,
                resolveSettingsProfileElements(
                    /* default_profile_id= */ {},
                    profiles,
                    UUIDHelpers::Nil,
                    roles_info.enabled_roles,
                    roles_info.settings_from_enabled_roles,
                    /* settings_from_user= */ {})
                    .elements);
        }

        ResolvedSettings resolveForProfile(const UUID & profile_id) const
        {
            SettingsProfileElements elements;
            elements.emplace_back().parent_profile = profile_id;
            std::vector<UUID> profile_ids;
            std::vector<UUID> substituted_profile_ids;
            std::unordered_map<UUID, String> profile_names;
            auto get_profile = [this](const UUID & id) -> SettingsProfilePtr
            {
                auto it = profiles.find(id);
                return it == profiles.end() ? nullptr : it->second;
            };
            substituteProfiles(elements, get_profile, profile_ids, substituted_profile_ids, profile_names);
            return foldElements(access_control, elements);
        }

        const RolesByID & allRoles() const { return roles; }
        const SettingsProfilesByID & allProfiles() const { return profiles; }

        /// Returns the entities whose effective settings can change after `pending`. Dependencies
        /// point from a setting carrier to the entity which uses it; profile targets point the other
        /// way, so both directions are recorded before walking the affected subgraph.
        AccessEntityIDs findAffectedEntities(const AccessGraph & after, const PendingAccessEntities & pending) const
        {
            UnorderedMultiMapWithMemoryTracking<UUID, UUID> dependents;
            AccessEntityIDs profiles_targeting_all;
            AccessEntityIDs default_profiles;

            auto add_graph = [&](const AccessGraph & graph)
            {
                auto add_dependencies = [&](const auto & entities)
                {
                    for (const auto & [id, entity] : entities)
                    {
                        for (const auto & dependency : entity->findDependencies())
                            dependents.emplace(dependency, id);
                    }
                };

                add_dependencies(graph.users);
                add_dependencies(graph.roles);
                add_dependencies(graph.profiles);

                for (const auto & [profile_id, profile] : graph.profiles)
                {
                    if (profile->to_roles.all)
                    {
                        profiles_targeting_all.emplace(profile_id);
                        continue;
                    }
                    for (const auto & target_id : profile->to_roles.ids)
                        dependents.emplace(profile_id, target_id);
                }

                if (graph.default_profile_id)
                    default_profiles.emplace(*graph.default_profile_id);
            };

            add_graph(*this);
            add_graph(after);

            AccessEntityIDs affected;
            VectorWithMemoryTracking<UUID> queue;
            auto add_affected = [&](const UUID & id)
            {
                if (affected.emplace(id).second)
                    queue.emplace_back(id);
            };
            for (const auto & item : pending)
                add_affected(item.first);

            auto add_all = [&](const auto & entities)
            {
                for (const auto & item : entities)
                    add_affected(item.first);
            };

            size_t position = 0;
            while (position != queue.size())
            {
                const UUID id = queue[position++];

                auto [begin, end] = dependents.equal_range(id);
                for (auto it = begin; it != end; ++it)
                    add_affected(it->second);

                if (profiles_targeting_all.contains(id))
                {
                    add_all(users);
                    add_all(after.users);
                    add_all(roles);
                    add_all(after.roles);
                }
                if (default_profiles.contains(id))
                {
                    add_all(users);
                    add_all(after.users);
                }
            }
            return affected;
        }

    private:
        const AccessControl & access_control;
        UsersByID users;
        VectorWithMemoryTracking<UUID> user_order;
        RolesByID roles;
        SettingsProfilesByID profiles;
        std::optional<UUID> default_profile_id;
        bool has_all_users;
    };

    Replacements findReplacements(const AccessGraph & before, const PendingAccessEntities & pending)
    {
        Replacements replacements;
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
        SetWithMemoryTracking<String> names;
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

    /// Whether the write can change which same-name user a login resolves to. A user that only one
    /// storage holds cannot shadow or unshadow anything, so its name needs no separate check.
    bool mayChangeVisibleUserByName(
        const AccessControl & access_control, const AccessEntityPtr & old_entity, const AccessEntityPtr & new_entity)
    {
        auto is_user = [](const AccessEntityPtr & entity)
        { return entity && entity->getType() == AccessEntityType::USER; };

        size_t storages_with_name = 0;
        if (is_user(old_entity))
            storages_with_name = access_control.countStoragesWithEntityName(AccessEntityType::USER, old_entity->getName());
        /// A rename onto a name another storage already holds creates the collision itself.
        if (is_user(new_entity) && (!is_user(old_entity) || old_entity->getName() != new_entity->getName()))
        {
            storages_with_name = std::max(
                storages_with_name, access_control.countStoragesWithEntityName(AccessEntityType::USER, new_entity->getName()) + 1);
        }
        return storages_with_name > 1;
    }

    bool
    changesOnlyUsers(const AccessControl & access_control, const PendingAccessEntities & pending, const PendingAccessEntities & current)
    {
        for (const auto & [id, new_entity] : pending)
        {
            auto current_it = current.find(id);
            auto old_entity = current_it == current.end() ? access_control.tryRead(id) : current_it->second;
            if ((old_entity && old_entity->getType() != AccessEntityType::USER)
                || (new_entity && new_entity->getType() != AccessEntityType::USER))
                return false;
        }
        return true;
    }

    void checkFeatureTierForPendingAccessEntitiesWithGraph(
        const AccessGraph & initial,
        const AccessControl & access_control,
        const PendingAccessEntities & pending,
        const PendingAccessEntities & current,
        bool new_users_are_shadowed)
    {
        auto refuse_if_restricted = [&](const String & setting_name)
        {
            if (auto reason = getFeatureTierRestriction(access_control, setting_name, settingGetTier(setting_name)))
                throw Exception(*reason, ErrorCodes::READONLY);
        };

        AccessGraph before = initial;
        before.apply(current);
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
            auto old_folded = old_elements ? foldElements(access_control, *old_elements) : ResolvedSettings{};
            for (const auto & [setting_name, element] : foldElements(access_control, *new_elements))
            {
                auto it = old_folded.find(setting_name);
                if (it != old_folded.end() && it->second == element)
                    continue;
                refuse_if_restricted(setting_name);
            }
        }

        AccessGraph after = before;
        after.apply(pending, new_users_are_shadowed);

        /// A login is resolved by name across storages, so creating, dropping or renaming a user can
        /// expose or hide a same-name user of another storage. Compare what each touched name resolves
        /// to instead of what each id holds.
        if (before.hasAllUsers())
        {
            SetWithMemoryTracking<String> touched_names;
            for (const auto & [id, entity] : pending)
            {
                if (auto old_user = before.getUser(id))
                    touched_names.emplace(old_user->getName());
                if (typeid_cast<const User *>(entity.get()))
                    touched_names.emplace(entity->getName());
            }

            for (const auto & name : touched_names)
            {
                auto [before_id, before_user] = before.visibleUser(name);
                auto [after_id, after_user] = after.visibleUser(name);
                /// Nobody can log in under a name no storage holds any more, and a name which keeps
                /// resolving to the same entity is checked by id below.
                if (!after_user || before_id == after_id)
                    continue;

                auto before_settings
                    = before_user ? before.resolveForUser(before_id, *before_user) : before.resolveForNewUser(after_id, name);
                checkResolvedSettings(access_control, before_settings, after.resolveForUser(after_id, *after_user));
            }
        }

        auto affected = before.findAffectedEntities(after, pending);

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

        auto check_role = [&](const UUID & role_id)
        {
            auto after_settings = after.resolveForRole(role_id);
            auto old_id = role_id;
            if (auto it = replacements.find(role_id); it != replacements.end())
                old_id = it->second;
            auto before_settings = before.allRoles().contains(old_id) ? before.resolveForRole(old_id) : ResolvedSettings{};
            checkResolvedSettings(access_control, before_settings, after_settings);
        };

        auto check_profile = [&](const UUID & profile_id)
        {
            auto after_settings = after.resolveForProfile(profile_id);
            auto old_id = profile_id;
            if (auto it = replacements.find(profile_id); it != replacements.end())
                old_id = it->second;
            auto before_settings = before.allProfiles().contains(old_id) ? before.resolveForProfile(old_id) : ResolvedSettings{};
            checkResolvedSettings(access_control, before_settings, after_settings);
        };

        bool changes_only_users = changesOnlyUsers(access_control, pending, current);
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
            {
                if (affected.contains(id))
                    check_user(id, user);
            }
            for (const auto & item : after.allRoles())
            {
                if (affected.contains(item.first))
                    check_role(item.first);
            }
            for (const auto & item : after.allProfiles())
            {
                if (affected.contains(item.first))
                    check_profile(item.first);
            }
        }
    }
}


FeatureTierAccessEntityChecker prepareFeatureTierAccessEntityChecker(
    const AccessControl & access_control,
    const PendingAccessEntities & pending,
    const PendingAccessEntities & current,
    bool force,
    bool new_users_are_shadowed)
{
    if (!isAnyFeatureTierRestricted(access_control))
        return {};

    bool may_change_visible_user = false;
    if (!force)
    {
        bool relevant = false;
        for (const auto & [id, entity] : pending)
        {
            auto current_it = current.find(id);
            auto old_entity = current_it == current.end() ? access_control.tryRead(id) : current_it->second;
            if (mayChangeVisibleUserByName(access_control, old_entity, entity))
                may_change_visible_user = true;
            else if (!mayChangeSettingsInEffect(old_entity, entity))
                continue;
            relevant = true;
        }
        if (!relevant)
            return {};
    }

    /// Resolving a name needs every user, not only the ones the write names.
    bool changes_only_users = !force && !may_change_visible_user && changesOnlyUsers(access_control, pending, current);
    auto graph = std::make_shared<AccessGraph>(access_control, !changes_only_users, pending, current);
    return [&access_control, graph, new_users_are_shadowed](
               const PendingAccessEntities & pending_, const PendingAccessEntities & current_)
    {
        checkFeatureTierForPendingAccessEntitiesWithGraph(*graph, access_control, pending_, current_, new_users_are_shadowed);
    };
}


void checkFeatureTierForPendingAccessEntities(
    const AccessControl & access_control,
    const PendingAccessEntities & pending,
    const PendingAccessEntities & current,
    bool new_users_are_shadowed)
{
    auto checker
        = prepareFeatureTierAccessEntityChecker(access_control, pending, current, /* force= */ false, new_users_are_shadowed);
    if (checker)
        checker(pending, current);
}


void checkFeatureTierForVisibleUserChange(
    const AccessControl & access_control, const UUID & before_user_id, const UUID & after_user_id)
{
    if (!isAnyFeatureTierRestricted(access_control))
        return;

    AccessGraph graph(access_control, /* read_all_users= */ true, /* pending= */ {}, /* current= */ {});
    auto before_user = graph.getUser(before_user_id);
    auto after_user = graph.getUser(after_user_id);
    if (!before_user || !after_user)
        return;

    checkResolvedSettings(
        access_control, graph.resolveForUser(before_user_id, *before_user), graph.resolveForUser(after_user_id, *after_user));
}

}
