#pragma once

#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>
#include <memory>
#include <optional>


namespace DB
{
class ASTGenericRoleSet;
class AccessControlManager;


/// Represents a set of users/roles like
/// {user_name | role_name | CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
/// Similar to ASTGenericRoleSet, but with IDs instead of names.
struct GenericRoleSet
{
    GenericRoleSet();
    GenericRoleSet(const GenericRoleSet & src);
    GenericRoleSet & operator =(const GenericRoleSet & src);
    GenericRoleSet(GenericRoleSet && src);
    GenericRoleSet & operator =(GenericRoleSet && src);

    struct AllTag {};
    GenericRoleSet(AllTag);

    GenericRoleSet(const UUID & id);
    GenericRoleSet(const std::vector<UUID> & ids_);
    GenericRoleSet(const boost::container::flat_set<UUID> & ids_);

    /// The constructor from AST requires the AccessControlManager if `ast.id_mode == false`.
    GenericRoleSet(const ASTGenericRoleSet & ast);
    GenericRoleSet(const ASTGenericRoleSet & ast, const UUID & current_user_id);
    GenericRoleSet(const ASTGenericRoleSet & ast, const AccessControlManager & manager);
    GenericRoleSet(const ASTGenericRoleSet & ast, const AccessControlManager & manager, const UUID & current_user_id);

    std::shared_ptr<ASTGenericRoleSet> toAST() const;
    String toString() const;
    Strings toStrings() const;

    std::shared_ptr<ASTGenericRoleSet> toASTWithNames(const AccessControlManager & manager) const;
    String toStringWithNames(const AccessControlManager & manager) const;
    Strings toStringsWithNames(const AccessControlManager & manager) const;

    bool empty() const;
    void clear();
    void add(const UUID & id);
    void add(const std::vector<UUID> & ids_);
    void add(const boost::container::flat_set<UUID> & ids_);

    /// Checks if a specified ID matches this GenericRoleSet.
    bool match(const UUID & id) const;
    bool match(const UUID & user_id, const std::vector<UUID> & enabled_roles) const;
    bool match(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles) const;

    /// Returns a list of matching IDs. The function must not be called if `all` == `true`.
    std::vector<UUID> getMatchingIDs() const;

    /// Returns a list of matching users.
    std::vector<UUID> getMatchingUsers(const AccessControlManager & manager) const;
    std::vector<UUID> getMatchingRoles(const AccessControlManager & manager) const;
    std::vector<UUID> getMatchingUsersAndRoles(const AccessControlManager & manager) const;

    friend bool operator ==(const GenericRoleSet & lhs, const GenericRoleSet & rhs);
    friend bool operator !=(const GenericRoleSet & lhs, const GenericRoleSet & rhs) { return !(lhs == rhs); }

    boost::container::flat_set<UUID> ids;
    bool all = false;
    boost::container::flat_set<UUID> except_ids;

private:
    void init(const ASTGenericRoleSet & ast, const AccessControlManager * manager = nullptr, const UUID * current_user_id = nullptr);
};

}
