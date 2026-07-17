#pragma once

#include <Core/UUID.h>
#include <Core/Types.h>
#include <boost/container/flat_set.hpp>
#include <memory>
#include <optional>
#include <unordered_map>


namespace DB
{
class ASTRolesOrUsersSet;
class AccessControl;


/// Represents a set of users/roles like
/// {user_name | role_name | CURRENT_USER | ALL | NONE} [,...]
/// [EXCEPT {user_name | role_name | CURRENT_USER | ALL | NONE} [,...]]
/// Similar to ASTRolesOrUsersSet, but with IDs instead of names.
struct RolesOrUsersSet
{
    RolesOrUsersSet();
    RolesOrUsersSet(const RolesOrUsersSet & src);
    RolesOrUsersSet & operator =(const RolesOrUsersSet & src);
    RolesOrUsersSet(RolesOrUsersSet && src) noexcept;
    RolesOrUsersSet & operator =(RolesOrUsersSet && src) noexcept;

    struct AllTag {};
    RolesOrUsersSet(AllTag); /// NOLINT

    RolesOrUsersSet(const UUID & id); /// NOLINT
    RolesOrUsersSet(const std::vector<UUID> & ids_); /// NOLINT

    /// The constructor from AST requires the AccessControl if `ast.id_mode == false`.
    RolesOrUsersSet(const ASTRolesOrUsersSet & ast); /// NOLINT
    RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const std::optional<UUID> & current_user_id);
    RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const AccessControl & access_control);
    RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const AccessControl & access_control, const std::optional<UUID> & current_user_id);

    std::shared_ptr<ASTRolesOrUsersSet> toAST() const;
    std::shared_ptr<ASTRolesOrUsersSet> toASTWithNames(const AccessControl & access_control) const;

    String toString() const;
    String toStringWithNames(const AccessControl & access_control) const;
    Strings toStringsWithNames(const AccessControl & access_control) const;

    bool empty() const;
    void clear();
    void add(const UUID & id);
    void add(const std::vector<UUID> & ids_);

    /// Checks if a specified ID matches this RolesOrUsersSet.
    bool match(const UUID & id) const;
    bool match(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles) const;

    /// Returns a list of matching IDs. The function must not be called if `all` == `true`.
    std::vector<UUID> getMatchingIDs() const;

    /// Returns a list of matching users and roles.
    std::vector<UUID> getMatchingIDs(const AccessControl & access_control) const;

    friend bool operator ==(const RolesOrUsersSet & lhs, const RolesOrUsersSet & rhs);
    friend bool operator !=(const RolesOrUsersSet & lhs, const RolesOrUsersSet & rhs) { return !(lhs == rhs); }

    std::vector<UUID> findDependencies() const;
    bool hasDependencies(const std::unordered_set<UUID> & dependencies_ids) const;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids);
    void copyDependenciesFrom(const RolesOrUsersSet & src, const std::unordered_set<UUID> & dependencies_ids);
    void removeDependencies(const std::unordered_set<UUID> & dependencies_ids);

    bool all = false;
    boost::container::flat_set<UUID> ids;
    boost::container::flat_set<UUID> except_ids;

private:
    void init(const ASTRolesOrUsersSet & ast, const AccessControl * access_control = nullptr, const std::optional<UUID> & current_user_id = {});
};

}
