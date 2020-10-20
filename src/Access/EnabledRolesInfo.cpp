#include <Access/EnabledRolesInfo.h>


namespace DB
{

Strings EnabledRolesInfo::getCurrentRolesNames() const
{
    Strings result;
    result.reserve(current_roles.size());
    for (const auto & id : current_roles)
        result.emplace_back(names_of_roles.at(id));
    return result;
}


Strings EnabledRolesInfo::getEnabledRolesNames() const
{
    Strings result;
    result.reserve(enabled_roles.size());
    for (const auto & id : enabled_roles)
        result.emplace_back(names_of_roles.at(id));
    return result;
}


bool operator==(const EnabledRolesInfo & lhs, const EnabledRolesInfo & rhs)
{
    return (lhs.current_roles == rhs.current_roles) && (lhs.enabled_roles == rhs.enabled_roles)
        && (lhs.enabled_roles_with_admin_option == rhs.enabled_roles_with_admin_option) && (lhs.names_of_roles == rhs.names_of_roles)
        && (lhs.access == rhs.access) && (lhs.settings_from_enabled_roles == rhs.settings_from_enabled_roles);
}

}
