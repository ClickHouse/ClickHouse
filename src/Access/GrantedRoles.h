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

    [[nodiscard]] bool isEmpty() const { return roles.empty(); }

    [[nodiscard]] bool isGranted(const UUID & role_) const;
    [[nodiscard]] bool isGrantedWithAdminOption(const UUID & role_) const;

    [[nodiscard]] const boost::container::flat_set<UUID> & getGranted() const { return roles; }
    [[nodiscard]] const boost::container::flat_set<UUID> & getGrantedWithAdminOption() const { return roles_with_admin_option; }

    [[nodiscard]] std::vector<UUID> findGranted(const std::vector<UUID> & ids) const;
    [[nodiscard]] std::vector<UUID> findGranted(const boost::container::flat_set<UUID> & ids) const;
    [[nodiscard]] std::vector<UUID> findGranted(const RolesOrUsersSet & ids) const;
    [[nodiscard]] std::vector<UUID> findGrantedWithAdminOption(const std::vector<UUID> & ids) const;
    [[nodiscard]] std::vector<UUID> findGrantedWithAdminOption(const boost::container::flat_set<UUID> & ids) const;
    [[nodiscard]] std::vector<UUID> findGrantedWithAdminOption(const RolesOrUsersSet & ids) const;

    struct Element
    {
        std::vector<UUID> ids;
        bool admin_option = false;
        [[nodiscard]] bool empty() const { return ids.empty(); }
    };
    using Elements = std::vector<Element>;

    /// Retrieves the information about grants.
    [[nodiscard]] Elements getElements() const;

    void makeUnion(const GrantedRoles & other);
    void makeIntersection(const GrantedRoles & other);

    friend bool operator ==(const GrantedRoles & left, const GrantedRoles & right) { return (left.roles == right.roles) && (left.roles_with_admin_option == right.roles_with_admin_option); }
    friend bool operator !=(const GrantedRoles & left, const GrantedRoles & right) { return !(left == right); }

    [[nodiscard]] std::vector<UUID> findDependencies() const;
    [[nodiscard]] bool hasDependencies(const std::unordered_set<UUID> & ids) const;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids);
    void copyDependenciesFrom(const GrantedRoles & src, const std::unordered_set<UUID> & ids);
    void removeDependencies(const std::unordered_set<UUID> & ids);

private:
    boost::container::flat_set<UUID> roles;
    boost::container::flat_set<UUID> roles_with_admin_option;
};
}
