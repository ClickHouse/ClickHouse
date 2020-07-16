#include <Access/Role.h>


namespace DB
{

bool Role::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_role = typeid_cast<const Role &>(other);
    return (access == other_role.access) && (granted_roles == other_role.granted_roles) && (settings == other_role.settings);
}

}
