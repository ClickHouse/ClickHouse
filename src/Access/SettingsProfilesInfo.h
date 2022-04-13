#pragma once

#include <Access/SettingsConstraints.h>
#include <Common/SettingsChanges.h>
#include <Core/UUID.h>
#include <unordered_map>


namespace DB
{
struct SettingsConstraintsAndProfileIDs;

/// Information about the default settings which are applied to an user on login.
struct SettingsProfilesInfo
{
    SettingsChanges settings;
    SettingsConstraints constraints;

    /// Profiles explicitly assigned to the user.
    std::vector<UUID> profiles;

    /// Profiles assigned to the user both explicitly and implicitly.
    /// Implicitly assigned profiles include parent profiles of other assigned profiles,
    /// profiles assigned via granted roles, profiles assigned via their own settings,
    /// and the main default profile (see the section `default_profile` in the main configuration file).
    /// The order of IDs in this vector corresponds the order of applying of these profiles.
    std::vector<UUID> profiles_with_implicit;

    /// Names of all the profiles in `profiles`.
    std::unordered_map<UUID, String> names_of_profiles;

    explicit SettingsProfilesInfo(const AccessControl & access_control_) : constraints(access_control_), access_control(access_control_) {}
    std::shared_ptr<const SettingsConstraintsAndProfileIDs> getConstraintsAndProfileIDs(
        const std::shared_ptr<const SettingsConstraintsAndProfileIDs> & previous = nullptr) const;

    friend bool operator ==(const SettingsProfilesInfo & lhs, const SettingsProfilesInfo & rhs);
    friend bool operator !=(const SettingsProfilesInfo & lhs, const SettingsProfilesInfo & rhs) { return !(lhs == rhs); }

    Strings getProfileNames() const
    {
        Strings result;
        result.reserve(profiles.size());
        for (const auto & profile_id : profiles)
            result.push_back(names_of_profiles.at(profile_id));

        return result;
    }

private:
    const AccessControl & access_control;
};

}
