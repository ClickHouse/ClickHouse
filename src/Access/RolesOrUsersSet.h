#pragma once

#include <Core/UUID.h>
#include <Core/Types.h>
#include <boost/container/flat_set.hpp>
#include <memory>
#include <optional>


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
    RolesOrUsersSet(RolesOrUsersSet && src);
    RolesOrUsersSet & operator =(RolesOrUsersSet && src);

    struct AllTag {};
    RolesOrUsersSet(AllTag);

    RolesOrUsersSet(const UUID & id);
    RolesOrUsersSet(const std::vector<UUID> & ids_);

    /// The constructor from AST requires the AccessControl if `ast.id_mode == false`.
    RolesOrUsersSet(const ASTRolesOrUsersSet & ast);
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

    /// Returns true if this set contains each element of another set.
    bool contains(const RolesOrUsersSet & other) const;

    friend bool operator ==(const RolesOrUsersSet & lhs, const RolesOrUsersSet & rhs);
    friend bool operator !=(const RolesOrUsersSet & lhs, const RolesOrUsersSet & rhs) { return !(lhs == rhs); }

    bool all = false;
    boost::container::flat_set<UUID> ids;
    boost::container::flat_set<UUID> except_ids;

private:
    void init(const ASTRolesOrUsersSet & ast, const AccessControl * access_control = nullptr, const std::optional<UUID> & current_user_id = {});
};

}
