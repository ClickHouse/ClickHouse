#include <Access/GrantedRoles.h>
#include <Access/RolesOrUsersSet.h>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
void GrantedRoles::grant(const UUID & role_)
{
    roles.insert(role_);
}

void GrantedRoles::grant(const std::vector<UUID> & roles_)
{
    roles.insert(roles_.begin(), roles_.end());
}

void GrantedRoles::grantWithAdminOption(const UUID & role_)
{
    roles.insert(role_);
    roles_with_admin_option.insert(role_);
}

void GrantedRoles::grantWithAdminOption(const std::vector<UUID> & roles_)
{
    roles.insert(roles_.begin(), roles_.end());
    roles_with_admin_option.insert(roles_.begin(), roles_.end());
}


void GrantedRoles::revoke(const UUID & role_)
{
    roles.erase(role_);
    roles_with_admin_option.erase(role_);
}

void GrantedRoles::revoke(const std::vector<UUID> & roles_)
{
    for (const UUID & role : roles_)
        revoke(role);
}

void GrantedRoles::revokeAdminOption(const UUID & role_)
{
    roles_with_admin_option.erase(role_);
}

void GrantedRoles::revokeAdminOption(const std::vector<UUID> & roles_)
{
    for (const UUID & role : roles_)
        revokeAdminOption(role);
}


bool GrantedRoles::isGranted(const UUID & role_) const
{
    return roles.count(role_);
}

bool GrantedRoles::isGrantedWithAdminOption(const UUID & role_) const
{
    return roles_with_admin_option.count(role_);
}


std::vector<UUID> GrantedRoles::findGranted(const std::vector<UUID> & ids) const
{
    std::vector<UUID> res;
    res.reserve(ids.size());
    for (const UUID & id : ids)
    {
        if (isGranted(id))
            res.push_back(id);
    }
    return res;
}

std::vector<UUID> GrantedRoles::findGranted(const boost::container::flat_set<UUID> & ids) const
{
    std::vector<UUID> res;
    res.reserve(ids.size());
    boost::range::set_intersection(ids, roles, std::back_inserter(res));
    return res;
}

std::vector<UUID> GrantedRoles::findGranted(const RolesOrUsersSet & ids) const
{
    std::vector<UUID> res;
    for (const UUID & id : roles)
    {
        if (ids.match(id))
            res.emplace_back(id);
    }
    return res;
}

std::vector<UUID> GrantedRoles::findGrantedWithAdminOption(const std::vector<UUID> & ids) const
{
    std::vector<UUID> res;
    res.reserve(ids.size());
    for (const UUID & id : ids)
    {
        if (isGrantedWithAdminOption(id))
            res.push_back(id);
    }
    return res;
}

std::vector<UUID> GrantedRoles::findGrantedWithAdminOption(const boost::container::flat_set<UUID> & ids) const
{
    std::vector<UUID> res;
    res.reserve(ids.size());
    boost::range::set_intersection(ids, roles_with_admin_option, std::back_inserter(res));
    return res;
}

std::vector<UUID> GrantedRoles::findGrantedWithAdminOption(const RolesOrUsersSet & ids) const
{
    std::vector<UUID> res;
    for (const UUID & id : roles_with_admin_option)
    {
        if (ids.match(id))
            res.emplace_back(id);
    }
    return res;
}


GrantedRoles::Elements GrantedRoles::getElements() const
{
    Elements elements;

    Element element;
    element.ids.reserve(roles.size());
    boost::range::set_difference(roles, roles_with_admin_option, std::back_inserter(element.ids));
    if (!element.empty())
    {
        element.admin_option = false; //-V1048
        elements.emplace_back(std::move(element));
    }

    if (!roles_with_admin_option.empty())
    {
        element = {};
        element.ids.insert(element.ids.end(), roles_with_admin_option.begin(), roles_with_admin_option.end());
        element.admin_option = true;
        elements.emplace_back(std::move(element));
    }

    return elements;
}


void GrantedRoles::makeUnion(const GrantedRoles & other)
{
    roles.insert(other.roles.begin(), other.roles.end());
    roles_with_admin_option.insert(other.roles_with_admin_option.begin(), other.roles_with_admin_option.end());
}

void GrantedRoles::makeIntersection(const GrantedRoles & other)
{
    boost::range::remove_erase_if(roles, [&other](const UUID & id) { return other.roles.find(id) == other.roles.end(); });

    boost::range::remove_erase_if(roles_with_admin_option, [&other](const UUID & id)
    {
        return other.roles_with_admin_option.find(id) == other.roles_with_admin_option.end();
    });
}
}
