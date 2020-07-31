#pragma once

#include <Access/AccessRights.h>
#include <Core/UUID.h>
#include <unordered_map>
#include <vector>


namespace DB
{

/// Information about a role.
struct CurrentRolesInfo
{
    std::vector<UUID> current_roles;
    std::vector<UUID> enabled_roles;
    std::vector<UUID> enabled_roles_with_admin_option;
    std::unordered_map<UUID, String> names_of_roles;
    AccessRights access;
    AccessRights access_with_grant_option;

    Strings getCurrentRolesNames() const;
    Strings getEnabledRolesNames() const;

    friend bool operator ==(const CurrentRolesInfo & lhs, const CurrentRolesInfo & rhs);
    friend bool operator !=(const CurrentRolesInfo & lhs, const CurrentRolesInfo & rhs) { return !(lhs == rhs); }
};

using CurrentRolesInfoPtr = std::shared_ptr<const CurrentRolesInfo>;

}
