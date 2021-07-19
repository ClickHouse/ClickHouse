#pragma once

#include <Common/SettingsChanges.h>
#include <Core/UUID.h>
#include <unordered_map>


namespace DB
{
class SettingsConstraints;

/// Information about the default settings which are applied to an user on login.
struct SettingsProfilesInfo
{
    SettingsProfilesInfo();
    SettingsProfilesInfo(SettingsChanges settings_,
                         SettingsConstraints constraints_,
                         std::vector<UUID> profiles_,
                         std::vector<UUID> profiles_including_implicit_,
                         std::unordered_map<UUID, String> names_of_profiles_);
    ~SettingsProfilesInfo();

    SettingsChanges settings;
    std::shared_ptr<const SettingsConstraints> constraints;

    /// Profiles explicitly assigned to the user.
    std::vector<UUID> profiles;

    /// Profiles assigned to the user both explicitly and implicitly.
    /// Implicitly assigned profiles include parent profiles of other assigned profiles,
    /// profiles assigned via granted roles, profiles assigned via their own settings,
    /// and the main default profile (see the section `default_profile` in the main configuration file).
    /// The order of IDs in this vector corresponds the order of applying of these profiles.
    std::vector<UUID> profiles_including_implicit;

    /// Names of all the profiles in `profiles_including_implicit`.
    std::unordered_map<UUID, String> names_of_profiles;

    friend bool operator ==(const SettingsProfilesInfo & lhs, const SettingsProfilesInfo & rhs);
    friend bool operator !=(const SettingsProfilesInfo & lhs, const SettingsProfilesInfo & rhs) { return !(lhs == rhs); }
};

}
