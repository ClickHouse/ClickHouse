#include <Access/SettingsProfilesCache.h>
#include <Access/AccessControlManager.h>
#include <Access/SettingsProfile.h>
#include <Core/Settings.h>
#include <Common/SettingsChanges.h>
#include <Common/quoteString.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int THERE_IS_NO_PROFILE;
}


SettingsProfilesCache::SettingsProfilesCache(const AccessControlManager & manager_)
    : manager(manager_) {}

SettingsProfilesCache::~SettingsProfilesCache() = default;


void SettingsProfilesCache::ensureAllProfilesRead()
{
    /// `mutex` is already locked.
    if (all_profiles_read)
        return;
    all_profiles_read = true;

    subscription = manager.subscribeForChanges<SettingsProfile>(
        [&](const UUID & id, const AccessEntityPtr & entity)
        {
            if (entity)
                profileAddedOrChanged(id, typeid_cast<SettingsProfilePtr>(entity));
            else
                profileRemoved(id);
        });

    for (const UUID & id : manager.findAll<SettingsProfile>())
    {
        auto profile = manager.tryRead<SettingsProfile>(id);
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
    settings_for_profiles.clear();
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
    settings_for_profiles.clear();
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
        throw Exception("Settings profile " + backQuote(default_profile_name) + " not found", ErrorCodes::THERE_IS_NO_PROFILE);

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
            mergeSettingsAndConstraintsFor(*enabled);
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

    merged_settings.merge(enabled.params.settings_from_enabled_roles);
    merged_settings.merge(enabled.params.settings_from_user);

    substituteProfiles(merged_settings);

    auto settings = merged_settings.toSettings();
    auto constraints = merged_settings.toSettingsConstraints(manager);
    enabled.setSettingsAndConstraints(
        std::make_shared<Settings>(std::move(settings)), std::make_shared<SettingsConstraints>(std::move(constraints)));
}


void SettingsProfilesCache::substituteProfiles(SettingsProfileElements & elements) const
{
    boost::container::flat_set<UUID> already_substituted;
    for (size_t i = 0; i != elements.size();)
    {
        auto & element = elements[i];
        if (!element.parent_profile)
        {
            ++i;
            continue;
        }

        auto parent_profile_id = *element.parent_profile;
        element.parent_profile.reset();
        if (already_substituted.count(parent_profile_id))
        {
            ++i;
            continue;
        }

        already_substituted.insert(parent_profile_id);
        auto parent_profile = all_profiles.find(parent_profile_id);
        if (parent_profile == all_profiles.end())
        {
            ++i;
            continue;
        }

        const auto & parent_profile_elements = parent_profile->second->elements;
        elements.insert(elements.begin() + i, parent_profile_elements.begin(), parent_profile_elements.end());
    }
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


std::shared_ptr<const SettingsChanges> SettingsProfilesCache::getProfileSettings(const String & profile_name)
{
    std::lock_guard lock{mutex};
    ensureAllProfilesRead();

    auto it = profiles_by_name.find(profile_name);
    if (it == profiles_by_name.end())
        throw Exception("Settings profile " + backQuote(profile_name) + " not found", ErrorCodes::THERE_IS_NO_PROFILE);
    const UUID profile_id = it->second;

    auto it2 = settings_for_profiles.find(profile_id);
    if (it2 != settings_for_profiles.end())
        return it2->second;

    SettingsProfileElements elements = all_profiles[profile_id]->elements;
    substituteProfiles(elements);
    auto res = std::make_shared<const SettingsChanges>(elements.toSettingsChanges());
    settings_for_profiles.emplace(profile_id, res);
    return res;
}


}
