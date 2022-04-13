#pragma once

#include <Access/SettingsConstraints.h>
#include <Core/UUID.h>
#include <vector>


namespace DB
{

/// Information about currently applied constraints and profiles.
struct SettingsConstraintsAndProfileIDs
{
    SettingsConstraints constraints;
    std::vector<UUID> current_profiles;
    std::vector<UUID> enabled_profiles;

    explicit SettingsConstraintsAndProfileIDs(const AccessControl & access_control_) : constraints(access_control_) {}
};

}
