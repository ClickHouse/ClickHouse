#include <Access/User.h>


namespace DB
{

bool User::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_user = typeid_cast<const User &>(other);
    return (authentication == other_user.authentication) && (allowed_client_hosts == other_user.allowed_client_hosts)
        && (access == other_user.access) && (access_with_grant_option == other_user.access_with_grant_option)
        && (granted_roles == other_user.granted_roles) && (granted_roles_with_admin_option == other_user.granted_roles_with_admin_option)
        && (default_roles == other_user.default_roles) && (profile == other_user.profile);
}

}
