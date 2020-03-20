#include <Access/Role.h>


namespace DB
{

bool Role::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_role = typeid_cast<const Role &>(other);
    return (access == other_role.access) && (access_with_grant_option == other_role.access_with_grant_option)
        && (granted_roles == other_role.granted_roles) && (granted_roles_with_admin_option == other_role.granted_roles_with_admin_option);
}

}
