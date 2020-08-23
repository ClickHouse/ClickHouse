#pragma once

#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>
#include <memory>
#include <optional>


namespace DB
{
class ASTRolesOrUsersSet;
class AccessControlManager;
class VisibleAccessEntities;


/// Represents a set of users/roles like
/// {user_name | role_name | CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
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

    /// The constructor from AST requires the AccessControlManager if `ast.id_mode == false`.
    RolesOrUsersSet(const ASTRolesOrUsersSet & ast);

private:
    RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const AccessControlManager & manager, const std::optional<UUID> & current_user_id);
public:
    RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const VisibleAccessEntities & visible_entities);

    std::shared_ptr<ASTRolesOrUsersSet> toAST() const;
private:
    std::shared_ptr<ASTRolesOrUsersSet> toASTWithNames(const AccessControlManager & manager) const;
public:
    std::shared_ptr<ASTRolesOrUsersSet> toASTWithNames(const VisibleAccessEntities & visible_entities) const;

    String toString() const;
private:
    String toStringWithNames(const AccessControlManager & manager) const;
public:
    String toStringWithNames(const VisibleAccessEntities & visible_entities) const;
private:
    Strings toStringsWithNames(const AccessControlManager & manager) const;
public:
    Strings toStringsWithNames(const VisibleAccessEntities & visible_entities) const;

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
private:
    std::vector<UUID> getMatchingIDs(const AccessControlManager & visible_entities) const;
public:
    std::vector<UUID> getMatchingIDs(const VisibleAccessEntities & visible_entities) const;

    friend bool operator ==(const RolesOrUsersSet & lhs, const RolesOrUsersSet & rhs);
    friend bool operator !=(const RolesOrUsersSet & lhs, const RolesOrUsersSet & rhs) { return !(lhs == rhs); }

    boost::container::flat_set<UUID> ids;
    bool all = false;
    boost::container::flat_set<UUID> except_ids;

private:
    void init(const ASTRolesOrUsersSet & ast, const AccessControlManager * manager, const VisibleAccessEntities * visible_entities, const std::optional<UUID> & current_user_id);
};

}
