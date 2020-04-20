#pragma once

#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>
#include <memory>
#include <optional>


namespace DB
{
class ASTExtendedRoleSet;
class AccessControlManager;


/// Represents a set of users/roles like
/// {user_name | role_name | CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
/// Similar to ASTExtendedRoleSet, but with IDs instead of names.
struct ExtendedRoleSet
{
    ExtendedRoleSet();
    ExtendedRoleSet(const ExtendedRoleSet & src);
    ExtendedRoleSet & operator =(const ExtendedRoleSet & src);
    ExtendedRoleSet(ExtendedRoleSet && src);
    ExtendedRoleSet & operator =(ExtendedRoleSet && src);

    struct AllTag {};
    ExtendedRoleSet(AllTag);

    ExtendedRoleSet(const UUID & id);
    ExtendedRoleSet(const std::vector<UUID> & ids_);

    /// The constructor from AST requires the AccessControlManager if `ast.id_mode == false`.
    ExtendedRoleSet(const ASTExtendedRoleSet & ast);
    ExtendedRoleSet(const ASTExtendedRoleSet & ast, const std::optional<UUID> & current_user_id);
    ExtendedRoleSet(const ASTExtendedRoleSet & ast, const AccessControlManager & manager);
    ExtendedRoleSet(const ASTExtendedRoleSet & ast, const AccessControlManager & manager, const std::optional<UUID> & current_user_id);

    std::shared_ptr<ASTExtendedRoleSet> toAST() const;
    String toString() const;
    Strings toStrings() const;

    std::shared_ptr<ASTExtendedRoleSet> toASTWithNames(const AccessControlManager & manager) const;
    String toStringWithNames(const AccessControlManager & manager) const;
    Strings toStringsWithNames(const AccessControlManager & manager) const;

    bool empty() const;
    void clear();
    void add(const UUID & id);
    void add(const std::vector<UUID> & ids_);

    /// Checks if a specified ID matches this ExtendedRoleSet.
    bool match(const UUID & id) const;
    bool match(const UUID & user_id, const std::vector<UUID> & enabled_roles) const;
    bool match(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles) const;

    /// Returns a list of matching IDs. The function must not be called if `all` == `true`.
    std::vector<UUID> getMatchingIDs() const;

    /// Returns a list of matching users and roles.
    std::vector<UUID> getMatchingIDs(const AccessControlManager & manager) const;

    friend bool operator ==(const ExtendedRoleSet & lhs, const ExtendedRoleSet & rhs);
    friend bool operator !=(const ExtendedRoleSet & lhs, const ExtendedRoleSet & rhs) { return !(lhs == rhs); }

    boost::container::flat_set<UUID> ids;
    bool all = false;
    boost::container::flat_set<UUID> except_ids;

private:
    void init(const ASTExtendedRoleSet & ast, const AccessControlManager * manager = nullptr, const std::optional<UUID> & current_user_id = {});
};

}
