#include <Access/GrantedRoles.h>
#include <boost/range/algorithm/set_algorithm.hpp>


namespace DB
{
void GrantedRoles::grant(const UUID & role)
{
    roles.insert(role);
}

void GrantedRoles::grant(const std::vector<UUID> & roles_)
{
    for (const UUID & role : roles_)
        grant(role);
}

void GrantedRoles::grantWithAdminOption(const UUID & role)
{
    roles.insert(role);
    roles_with_admin_option.insert(role);
}

void GrantedRoles::grantWithAdminOption(const std::vector<UUID> & roles_)
{
    for (const UUID & role : roles_)
        grantWithAdminOption(role);
}


void GrantedRoles::revoke(const UUID & role)
{
    roles.erase(role);
    roles_with_admin_option.erase(role);
}

void GrantedRoles::revoke(const std::vector<UUID> & roles_)
{
    for (const UUID & role : roles_)
        revoke(role);
}

void GrantedRoles::revokeAdminOption(const UUID & role)
{
    roles_with_admin_option.erase(role);
}

void GrantedRoles::revokeAdminOption(const std::vector<UUID> & roles_)
{
    for (const UUID & role : roles_)
        revokeAdminOption(role);
}


GrantedRoles::Grants GrantedRoles::getGrants() const
{
    Grants res;
    res.grants_with_admin_option.insert(res.grants_with_admin_option.end(), roles_with_admin_option.begin(), roles_with_admin_option.end());
    res.grants.reserve(roles.size() - roles_with_admin_option.size());
    boost::range::set_difference(roles, roles_with_admin_option, std::back_inserter(res.grants));
    return res;
}

}
