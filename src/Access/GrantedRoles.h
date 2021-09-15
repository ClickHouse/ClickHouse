#pragma once

#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>
#include <vector>


namespace DB
{
/// Roles when they are granted to a role or user.
/// Stores both the roles themselves and the roles with admin option.
struct GrantedRoles
{
    boost::container::flat_set<UUID> roles;
    boost::container::flat_set<UUID> roles_with_admin_option;

    void grant(const UUID & role);
    void grant(const std::vector<UUID> & roles_);
    void grantWithAdminOption(const UUID & role);
    void grantWithAdminOption(const std::vector<UUID> & roles_);

    void revoke(const UUID & role);
    void revoke(const std::vector<UUID> & roles_);
    void revokeAdminOption(const UUID & role);
    void revokeAdminOption(const std::vector<UUID> & roles_);

    struct Grants
    {
        std::vector<UUID> grants;
        std::vector<UUID> grants_with_admin_option;
    };

    /// Retrieves the information about grants.
    Grants getGrants() const;

    friend bool operator ==(const GrantedRoles & left, const GrantedRoles & right) { return (left.roles == right.roles) && (left.roles_with_admin_option == right.roles_with_admin_option); }
    friend bool operator !=(const GrantedRoles & left, const GrantedRoles & right) { return !(left == right); }
};
}
