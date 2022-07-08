#pragma once

#include <Access/EnabledSettings.h>
#include <Poco/LRUCache.h>
#include <base/scope_guard.h>
#include <map>
#include <unordered_map>


namespace DB
{
class AccessControl;
struct SettingsProfile;
using SettingsProfilePtr = std::shared_ptr<const SettingsProfile>;
struct SettingsProfilesInfo;

/// Reads and caches all the settings profiles.
class SettingsProfilesCache
{
public:
    explicit SettingsProfilesCache(const AccessControl & access_control_);
    ~SettingsProfilesCache();

    void setDefaultProfileName(const String & default_profile_name);

    std::shared_ptr<const EnabledSettings> getEnabledSettings(
        const UUID & user_id,
        const SettingsProfileElements & settings_from_user_,
        const boost::container::flat_set<UUID> & enabled_roles,
        const SettingsProfileElements & settings_from_enabled_roles_);

    std::shared_ptr<const SettingsProfilesInfo> getSettingsProfileInfo(const UUID & profile_id);

private:
    void ensureAllProfilesRead();
    void profileAddedOrChanged(const UUID & profile_id, const SettingsProfilePtr & new_profile);
    void profileRemoved(const UUID & profile_id);
    void mergeSettingsAndConstraints();
    void mergeSettingsAndConstraintsFor(EnabledSettings & enabled) const;
    void substituteProfiles(SettingsProfileElements & elements, std::vector<UUID> & substituted_profiles, std::unordered_map<UUID, String> & names_of_substituted_profiles) const;

    const AccessControl & access_control;
    std::unordered_map<UUID, SettingsProfilePtr> all_profiles;
    std::unordered_map<String, UUID> profiles_by_name;
    bool all_profiles_read = false;
    scope_guard subscription;
    std::map<EnabledSettings::Params, std::weak_ptr<EnabledSettings>> enabled_settings;
    std::optional<UUID> default_profile_id;
    Poco::LRUCache<UUID, std::shared_ptr<const SettingsProfilesInfo>> profile_infos_cache;
    mutable std::mutex mutex;
};
}
