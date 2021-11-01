#include <Access/User.h>


namespace DB
{

bool User::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_user = typeid_cast<const User &>(other);
    return (auth_data == other_user.auth_data) && (allowed_client_hosts == other_user.allowed_client_hosts)
        && (access == other_user.access) && (granted_roles == other_user.granted_roles) && (default_roles == other_user.default_roles)
        && (settings == other_user.settings) && (grantees == other_user.grantees) && (default_database == other_user.default_database);
}

}
