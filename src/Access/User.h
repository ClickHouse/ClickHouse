#pragma once

#include <Access/IAccessEntity.h>
#include <Access/AccessRights.h>
#include <Access/Common/AuthenticationData.h>
#include <Access/Common/AllowedClientHosts.h>
#include <Access/GrantedRoles.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/SettingsProfileElement.h>


namespace DB
{
/** User and ACL.
  */
struct User : public IAccessEntity
{
    AuthenticationData auth_data;
    AllowedClientHosts allowed_client_hosts = AllowedClientHosts::AnyHostTag{};
    AccessRights access;
    GrantedRoles granted_roles;
    RolesOrUsersSet default_roles = RolesOrUsersSet::AllTag{};
    SettingsProfileElements settings;
    RolesOrUsersSet grantees = RolesOrUsersSet::AllTag{};
    String default_database;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<User>(); }
    static constexpr const auto TYPE = AccessEntityType::USER;
    AccessEntityType getType() const override { return TYPE; }
    void setName(const String & name_) override;
};

using UserPtr = std::shared_ptr<const User>;
}
