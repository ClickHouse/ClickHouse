#include <Access/SettingsProfilesCache.h>
#include <Access/AccessControl.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int THERE_IS_NO_PROFILE;
}

SettingsProfilesCache::SettingsProfilesCache(const AccessControl & access_control_)
    : access_control(access_control_) {}

SettingsProfilesCache::~SettingsProfilesCache() = default;


void SettingsProfilesCache::ensureAllProfilesRead()
{
    /// `mutex` is already locked.
    if (all_profiles_read)
        return;
    all_profiles_read = true;

    subscription = access_control.subscribeForChanges<SettingsProfile>(
        [&](const UUID & id, const AccessEntityPtr & entity)
        {
            if (entity)
                profileAddedOrChanged(id, typeid_cast<SettingsProfilePtr>(entity));
            else
                profileRemoved(id);
        });

    for (const UUID & id : access_control.findAll<SettingsProfile>())
    {
        auto profile = access_control.tryRead<SettingsProfile>(id);
        if (profile)
        {
            all_profiles.emplace(id, profile);
            profiles_by_name[profile->getName()] = id;
        }
    }
}


void SettingsProfilesCache::profileAddedOrChanged(const UUID & profile_id, const SettingsProfilePtr & new_profile)
{
    std::lock_guard lock{mutex};
    auto it = all_profiles.find(profile_id);
    if (it == all_profiles.end())
    {
        all_profiles.emplace(profile_id, new_profile);
        profiles_by_name[new_profile->getName()] = profile_id;
    }
    else
    {
        auto old_profile = it->second;
        it->second = new_profile;
        if (old_profile->getName() != new_profile->getName())
            profiles_by_name.erase(old_profile->getName());
        profiles_by_name[new_profile->getName()] = profile_id;
    }
    profile_infos_cache.clear();
    mergeSettingsAndConstraints();
}


void SettingsProfilesCache::profileRemoved(const UUID & profile_id)
{
    std::lock_guard lock{mutex};
    auto it = all_profiles.find(profile_id);
    if (it == all_profiles.end())
        return;
    profiles_by_name.erase(it->second->getName());
    all_profiles.erase(it);
    profile_infos_cache.clear();
    mergeSettingsAndConstraints();
}


void SettingsProfilesCache::setDefaultProfileName(const String & default_profile_name)
{
    std::lock_guard lock{mutex};
    ensureAllProfilesRead();

    if (default_profile_name.empty())
    {
        default_profile_id = {};
        return;
    }

    auto it = profiles_by_name.find(default_profile_name);
    if (it == profiles_by_name.end())
        throw Exception(ErrorCodes::THERE_IS_NO_PROFILE, "Settings profile {} not found", backQuote(default_profile_name));

    default_profile_id = it->second;
}


void SettingsProfilesCache::mergeSettingsAndConstraints()
{
    /// `mutex` is already locked.
    for (auto i = enabled_settings.begin(), e = enabled_settings.end(); i != e;)
    {
        auto enabled = i->second.lock();
        if (!enabled)
            i = enabled_settings.erase(i);
        else
        {
            try
            {
                mergeSettingsAndConstraintsFor(*enabled);
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::ACCESS_DENIED)
                {
                    /// A config-defined profile is now blocked for this SQL user
                    /// (disallow_config_defined_profiles_for_sql_defined_users was enabled).
                    /// Keep the existing settings for this session and continue with other entries.
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
                else
                    throw;
            }
            ++i;
        }
    }
}


void SettingsProfilesCache::mergeSettingsAndConstraintsFor(EnabledSettings & enabled) const
{
    SettingsProfileElements merged_settings;
    if (default_profile_id)
    {
        SettingsProfileElement new_element;
        new_element.parent_profile = *default_profile_id;
        merged_settings.emplace_back(new_element);
    }

    for (const auto & [profile_id, profile] : all_profiles)
        if (profile->to_roles.match(enabled.params.user_id, enabled.params.enabled_roles))
        {
            SettingsProfileElement new_element;
            new_element.parent_profile = profile_id;
            merged_settings.emplace_back(new_element);
        }

    merged_settings.merge(enabled.params.settings_from_enabled_roles, /* normalize= */ false);
    merged_settings.merge(enabled.params.settings_from_user, /* normalize= */ false);

    if (access_control.isConfigDefinedProfilesForSQLUsersDisallowed())
    {
        auto user_storage = access_control.findStorage(enabled.params.user_id);
        bool is_sql_defined_user = user_storage
            && strcmp(user_storage->getStorageType(), UsersConfigAccessStorage::STORAGE_TYPE) != 0;

        if (is_sql_defined_user)
        {
            /// Recursively check profiles from user and role settings, including
            /// indirect inheritance (e.g. SQL profile -> config profile).
            /// Default profile and TO-clause profiles are exempt (admin-configured).
            boost::container::flat_set<UUID> visited;
            std::vector<UUID> stack;

            auto collect_profile_ids = [](const SettingsProfileElements & elements, std::vector<UUID> & out)
            {
                for (const auto & elem : elements)
                    if (elem.parent_profile)
                        out.push_back(*elem.parent_profile);
            };
            collect_profile_ids(enabled.params.settings_from_user, stack);
            collect_profile_ids(enabled.params.settings_from_enabled_roles, stack);

            while (!stack.empty())
            {
                auto profile_id = stack.back();
                stack.pop_back();
                if (!visited.insert(profile_id).second)
                    continue;
                /// The server's default profile is always allowed.
                if (default_profile_id && profile_id == *default_profile_id)
                    continue;
                auto profile_storage = access_control.findStorage(profile_id);
                if (profile_storage
                    && strcmp(profile_storage->getStorageType(), UsersConfigAccessStorage::STORAGE_TYPE) == 0)
                {
                    auto profile_name = access_control.tryReadName(profile_id);
                    throw Exception(ErrorCodes::ACCESS_DENIED,
                        "Cannot apply config-defined profile '{}' to SQL-defined user. "
                        "Server setting `disallow_config_defined_profiles_for_sql_defined_users` is enabled",
                        profile_name.value_or("unknown"));
                }
                /// Walk into child profiles.
                auto it = all_profiles.find(profile_id);
                if (it != all_profiles.end())
                    collect_profile_ids(it->second->elements, stack);
            }
        }
    }

    auto info = std::make_shared<SettingsProfilesInfo>(access_control);

    substituteProfiles(merged_settings, info->profiles, info->profiles_with_implicit, info->names_of_profiles);

    info->settings = merged_settings.toSettingsChanges();
    info->constraints = merged_settings.toSettingsConstraints(access_control);

    enabled.setInfo(std::move(info));
}


void SettingsProfilesCache::substituteProfiles(
    SettingsProfileElements & elements,
    std::vector<UUID> & profiles,
    std::vector<UUID> & substituted_profiles,
    std::unordered_map<UUID, String> & names_of_substituted_profiles) const
{
    profiles = elements.toProfileIDs();

    /// We should substitute profiles in reversive order because the same profile can occur
    /// in `elements` multiple times (with some other settings in between) and in this case
    /// the last occurrence should override all the previous ones.
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

        auto profile_it = all_profiles.find(profile_id);
        if (profile_it == all_profiles.end())
            continue;

        const auto & profile = profile_it->second;
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

std::shared_ptr<const EnabledSettings> SettingsProfilesCache::getEnabledSettings(
    const UUID & user_id,
    const SettingsProfileElements & settings_from_user,
    const boost::container::flat_set<UUID> & enabled_roles,
    const SettingsProfileElements & settings_from_enabled_roles)
{
    std::lock_guard lock{mutex};
    ensureAllProfilesRead();

    EnabledSettings::Params params;
    params.user_id = user_id;
    params.settings_from_user = settings_from_user;
    params.enabled_roles = enabled_roles;
    params.settings_from_enabled_roles = settings_from_enabled_roles;

    auto it = enabled_settings.find(params);
    if (it != enabled_settings.end())
    {
        auto from_cache = it->second.lock();
        if (from_cache)
            return from_cache;
        enabled_settings.erase(it);
    }

    std::shared_ptr<EnabledSettings> res(new EnabledSettings(params));
    enabled_settings.emplace(std::move(params), res);
    mergeSettingsAndConstraintsFor(*res);
    return res;
}


std::shared_ptr<const SettingsProfilesInfo> SettingsProfilesCache::getSettingsProfileInfo(const UUID & profile_id)
{
    std::lock_guard lock{mutex};
    ensureAllProfilesRead();

    if (auto pos = this->profile_infos_cache.get(profile_id))
        return *pos;

    SettingsProfileElements elements;
    auto & element = elements.emplace_back();
    element.parent_profile = profile_id;

    auto info = std::make_shared<SettingsProfilesInfo>(access_control);

    substituteProfiles(elements, info->profiles, info->profiles_with_implicit, info->names_of_profiles);
    info->settings = elements.toSettingsChanges();
    info->constraints.merge(elements.toSettingsConstraints(access_control));

    profile_infos_cache.add(profile_id, info);
    return info;
}

}
