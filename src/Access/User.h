#pragma once

#include <Access/IAccessEntity.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>
#include <Access/GrantedAccess.h>
#include <Access/GrantedRoles.h>
#include <Access/ExtendedRoleSet.h>
#include <Access/SettingsProfileElement.h>


namespace DB
{
/** User and ACL.
  */
struct User : public IAccessEntity
{
    Authentication authentication;
    AllowedClientHosts allowed_client_hosts = AllowedClientHosts::AnyHostTag{};
    GrantedAccess access;
    GrantedRoles granted_roles;
    ExtendedRoleSet default_roles = ExtendedRoleSet::AllTag{};
    SettingsProfileElements settings;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<User>(); }
};

using UserPtr = std::shared_ptr<const User>;
}
