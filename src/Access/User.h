#pragma once

#include <Access/IAccessEntity.h>
#include <Access/AccessRights.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>
#include <Access/GrantedRoles.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/SettingsProfileElement.h>


namespace DB
{
/** User and ACL.
  */
struct User : public IAccessEntity
{
    Authentication authentication;
    AllowedClientHosts allowed_client_hosts = AllowedClientHosts::AnyHostTag{};
    AccessRights access;
    GrantedRoles granted_roles;
    RolesOrUsersSet default_roles = RolesOrUsersSet::AllTag{};
    SettingsProfileElements settings;
    RolesOrUsersSet grantees = RolesOrUsersSet::AllTag{};

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<User>(); }
    static constexpr const Type TYPE = Type::USER;
    Type getType() const override { return TYPE; }
};

using UserPtr = std::shared_ptr<const User>;
}
