#pragma once

#include "src/Access/IAccessEntity.h"
#include "src/Access/Authentication.h"
#include "src/Access/AllowedClientHosts.h"
#include "src/Access/GrantedAccess.h"
#include "src/Access/GrantedRoles.h"
#include "src/Access/RolesOrUsersSet.h"
#include "src/Access/SettingsProfileElement.h"


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
    RolesOrUsersSet default_roles = RolesOrUsersSet::AllTag{};
    SettingsProfileElements settings;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<User>(); }
    static constexpr const Type TYPE = Type::USER;
    Type getType() const override { return TYPE; }
};

using UserPtr = std::shared_ptr<const User>;
}
