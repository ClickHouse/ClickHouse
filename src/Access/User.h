#pragma once

#include <Access/IAccessEntity.h>
#include <Access/AccessRights.h>
#include <Access/AuthenticationData.h>
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
    std::vector<AuthenticationData> authentication_methods;
    AllowedClientHosts allowed_client_hosts = AllowedClientHosts::AnyHostTag{};
    AccessRights access;
    GrantedRoles granted_roles;
    RolesOrUsersSet default_roles = RolesOrUsersSet::AllTag{};
    SettingsProfileElements settings;
    RolesOrUsersSet grantees = RolesOrUsersSet::AllTag{};
    String default_database;
    time_t valid_until = 0;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<User>(); }
    static constexpr const auto TYPE = AccessEntityType::USER;
    AccessEntityType getType() const override { return TYPE; }
    void setName(const String & name_) override;

    std::vector<UUID> findDependencies() const override;
    void doReplaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) override;
    bool isBackupAllowed() const override { return settings.isBackupAllowed(); }
};

using UserPtr = std::shared_ptr<const User>;
}
