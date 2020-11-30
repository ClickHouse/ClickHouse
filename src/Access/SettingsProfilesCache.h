#pragma once

#include <Access/EnabledSettings.h>
#include <Core/UUID.h>
#include <Core/Types.h>
#include <ext/scope_guard.h>
#include <map>
#include <unordered_map>


namespace DB
{
class AccessControlManager;
struct SettingsProfile;
using SettingsProfilePtr = std::shared_ptr<const SettingsProfile>;
class SettingsProfileElements;
class EnabledSettings;


/// Reads and caches all the settings profiles.
class SettingsProfilesCache
{
public:
    SettingsProfilesCache(const AccessControlManager & manager_);
    ~SettingsProfilesCache();

    void setDefaultProfileName(const String & default_profile_name);

    std::shared_ptr<const EnabledSettings> getEnabledSettings(
        const UUID & user_id,
        const SettingsProfileElements & settings_from_user_,
        const boost::container::flat_set<UUID> & enabled_roles,
        const SettingsProfileElements & settings_from_enabled_roles_);

    std::shared_ptr<const SettingsChanges> getProfileSettings(const String & profile_name);

private:
    void ensureAllProfilesRead();
    void profileAddedOrChanged(const UUID & profile_id, const SettingsProfilePtr & new_profile);
    void profileRemoved(const UUID & profile_id);
    void mergeSettingsAndConstraints();
    void mergeSettingsAndConstraintsFor(EnabledSettings & enabled) const;
    void substituteProfiles(SettingsProfileElements & elements) const;

    const AccessControlManager & manager;
    std::unordered_map<UUID, SettingsProfilePtr> all_profiles;
    std::unordered_map<String, UUID> profiles_by_name;
    bool all_profiles_read = false;
    ext::scope_guard subscription;
    std::map<EnabledSettings::Params, std::weak_ptr<EnabledSettings>> enabled_settings;
    std::optional<UUID> default_profile_id;
    std::unordered_map<UUID, std::shared_ptr<const SettingsChanges>> settings_for_profiles;
    mutable std::mutex mutex;
};
}
