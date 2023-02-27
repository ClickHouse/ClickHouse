#pragma once

#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>
#include <vector>
#include <unordered_map>


namespace DB
{
struct RolesOrUsersSet;

/// Roles when they are granted to a role or user.
/// Stores both the roles themselves and the roles with admin option.
class GrantedRoles
{
public:
    void grant(const UUID & role_);
    void grant(const std::vector<UUID> & roles_);
    void grantWithAdminOption(const UUID & role_);
    void grantWithAdminOption(const std::vector<UUID> & roles_);

    void revoke(const UUID & role_);
    void revoke(const std::vector<UUID> & roles_);
    void revokeAdminOption(const UUID & role_);
    void revokeAdminOption(const std::vector<UUID> & roles_);

    bool isEmpty() const { return roles.empty(); }

    bool isGranted(const UUID & role_) const;
    bool isGrantedWithAdminOption(const UUID & role_) const;

    const boost::container::flat_set<UUID> & getGranted() const { return roles; }
    const boost::container::flat_set<UUID> & getGrantedWithAdminOption() const { return roles_with_admin_option; }

    std::vector<UUID> findGranted(const std::vector<UUID> & ids) const;
    std::vector<UUID> findGranted(const boost::container::flat_set<UUID> & ids) const;
    std::vector<UUID> findGranted(const RolesOrUsersSet & ids) const;
    std::vector<UUID> findGrantedWithAdminOption(const std::vector<UUID> & ids) const;
    std::vector<UUID> findGrantedWithAdminOption(const boost::container::flat_set<UUID> & ids) const;
    std::vector<UUID> findGrantedWithAdminOption(const RolesOrUsersSet & ids) const;

    struct Element
    {
        std::vector<UUID> ids;
        bool admin_option = false;
        bool empty() const { return ids.empty(); }
    };
    using Elements = std::vector<Element>;

    /// Retrieves the information about grants.
    Elements getElements() const;

    void makeUnion(const GrantedRoles & other);
    void makeIntersection(const GrantedRoles & other);

    friend bool operator ==(const GrantedRoles & left, const GrantedRoles & right) { return (left.roles == right.roles) && (left.roles_with_admin_option == right.roles_with_admin_option); }
    friend bool operator !=(const GrantedRoles & left, const GrantedRoles & right) { return !(left == right); }

    std::vector<UUID> findDependencies() const;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids);

private:
    boost::container::flat_set<UUID> roles;
    boost::container::flat_set<UUID> roles_with_admin_option;
};
}
