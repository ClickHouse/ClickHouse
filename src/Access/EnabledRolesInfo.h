#pragma once

#include <Access/AccessRights.h>
#include <Access/SettingsProfileElement.h>
#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>
#include <unordered_map>


namespace DB
{

/// Information about a role.
struct EnabledRolesInfo
{
    boost::container::flat_set<UUID> current_roles;
    boost::container::flat_set<UUID> enabled_roles;
    boost::container::flat_set<UUID> enabled_roles_with_admin_option;
    std::unordered_map<UUID, String> names_of_roles;
    AccessRights access;
    AccessRights access_with_grant_option;
    SettingsProfileElements settings_from_enabled_roles;

    Strings getCurrentRolesNames() const;
    Strings getEnabledRolesNames() const;

    friend bool operator ==(const EnabledRolesInfo & lhs, const EnabledRolesInfo & rhs);
    friend bool operator !=(const EnabledRolesInfo & lhs, const EnabledRolesInfo & rhs) { return !(lhs == rhs); }
};

}
