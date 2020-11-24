#pragma once

#include <Access/IAccessEntity.h>
#include <Access/AccessRights.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>
#include <Access/GrantedRoles.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/SettingsProfileElement.h>

#include <chrono>
#include <mutex>

namespace DB
{

/** Various cached data bound to a User instance. Access to any member must be synchronized via 'mutex' member.
  */
struct UserEtcCache
{
    mutable std::recursive_mutex mutex;
    std::size_t ldap_last_successful_password_check_params_hash = 0;
    std::chrono::steady_clock::time_point ldap_last_successful_password_check_timestamp;

    explicit UserEtcCache() = default;
    explicit UserEtcCache(const UserEtcCache & other) { (*this) = other; }
    explicit UserEtcCache(UserEtcCache && other) { (*this) = std::move(other); }
    UserEtcCache & operator= (const UserEtcCache & other);
    UserEtcCache & operator= (UserEtcCache && other);
};

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
    mutable UserEtcCache cache;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<User>(); }
    static constexpr const Type TYPE = Type::USER;
    Type getType() const override { return TYPE; }
};

using UserPtr = std::shared_ptr<const User>;
}
