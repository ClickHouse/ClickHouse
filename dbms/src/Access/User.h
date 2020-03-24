#pragma once

#include <Access/IAccessEntity.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>
#include <Access/AccessRights.h>
#include <Access/ExtendedRoleSet.h>
#include <Access/SettingsProfileElement.h>
#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>


namespace DB
{
/** User and ACL.
  */
struct User : public IAccessEntity
{
    Authentication authentication;
    AllowedClientHosts allowed_client_hosts = AllowedClientHosts::AnyHostTag{};
    AccessRights access;
    AccessRights access_with_grant_option;
    boost::container::flat_set<UUID> granted_roles;
    boost::container::flat_set<UUID> granted_roles_with_admin_option;
    ExtendedRoleSet default_roles = ExtendedRoleSet::AllTag{};
    SettingsProfileElements settings;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<User>(); }
};

using UserPtr = std::shared_ptr<const User>;
}
