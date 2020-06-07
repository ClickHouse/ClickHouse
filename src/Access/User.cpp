#include <Access/User.h>


namespace DB
{

bool User::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_user = typeid_cast<const User &>(other);

    return std::tie(authentication, allowed_client_hosts, access, granted_roles, default_roles, settings, allowed_proxy_users) ==
           std::tie(other_user.authentication, other_user.allowed_client_hosts, other_user.access, other_user.granted_roles, other_user.default_roles, other_user.settings, other_user.allowed_proxy_users);
}

}
