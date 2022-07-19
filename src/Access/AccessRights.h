#pragma once

#include <base/types.h>
#include <Access/Common/AccessRightsElement.h>
#include <functional>
#include <memory>
#include <vector>


namespace DB
{
/// Represents a set of access types granted on databases, tables, columns, etc.
/// For example, "GRANT SELECT, UPDATE ON db.*, GRANT INSERT ON db2.mytbl2" are access rights.
class AccessRights
{
public:
    AccessRights();
    explicit AccessRights(const AccessFlags & access);
    ~AccessRights();
    AccessRights(const AccessRights & src);
    AccessRights & operator =(const AccessRights & src);
    AccessRights(AccessRights && src) noexcept;
    AccessRights & operator =(AccessRights && src) noexcept;

    bool isEmpty() const;

    /// Revokes everything. It's the same as revoke(AccessType::ALL).
    void clear();

    /// Returns the information about all the access granted as a string.
    String toString() const;

    /// Returns the information about all the access granted.
    AccessRightsElements getElements() const;

    /// Grants access on a specified database/table/column.
    /// Does nothing if the specified access has been already granted.
    void grant(const AccessFlags & flags);
    void grant(const AccessFlags & flags, const std::string_view & database);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void grant(const AccessRightsElement & element);
    void grant(const AccessRightsElements & elements);

    void grantWithGrantOption(const AccessFlags & flags);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void grantWithGrantOption(const AccessRightsElement & element);
    void grantWithGrantOption(const AccessRightsElements & elements);

    /// Revokes a specified access granted earlier on a specified database/table/column.
    /// For example, revoke(AccessType::ALL) revokes all grants at all, just like clear();
    void revoke(const AccessFlags & flags);
    void revoke(const AccessFlags & flags, const std::string_view & database);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void revoke(const AccessRightsElement & element);
    void revoke(const AccessRightsElements & elements);

    void revokeGrantOption(const AccessFlags & flags);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void revokeGrantOption(const AccessRightsElement & element);
    void revokeGrantOption(const AccessRightsElements & elements);

    /// Whether a specified access granted.
    bool isGranted(const AccessFlags & flags) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(const AccessRightsElement & element) const;
    bool isGranted(const AccessRightsElements & elements) const;

    bool hasGrantOption(const AccessFlags & flags) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool hasGrantOption(const AccessRightsElement & element) const;
    bool hasGrantOption(const AccessRightsElements & elements) const;

    /// Merges two sets of access rights together.
    /// It's used to combine access rights from multiple roles.
    void makeUnion(const AccessRights & other);

    /// Makes an intersection of access rights.
    void makeIntersection(const AccessRights & other);

    /// Traverse the tree and modify each access flags.
    using ModifyFlagsFunction = std::function<AccessFlags(
        const AccessFlags & flags,
        const AccessFlags & min_flags_with_children,
        const AccessFlags & max_flags_with_children,
        const std::string_view & database,
        const std::string_view & table,
        const std::string_view & column)>;
    void modifyFlags(const ModifyFlagsFunction & function);
    void modifyFlagsWithGrantOption(const ModifyFlagsFunction & function);

    friend bool operator ==(const AccessRights & left, const AccessRights & right);
    friend bool operator !=(const AccessRights & left, const AccessRights & right) { return !(left == right); }

    /// Makes full access rights (GRANT ALL ON *.* WITH GRANT OPTION).
    static AccessRights getFullAccess();

private:
    template <bool with_grant_option, typename... Args>
    void grantImpl(const AccessFlags & flags, const Args &... args);

    template <bool with_grant_option>
    void grantImpl(const AccessRightsElement & element);

    template <bool with_grant_option>
    void grantImpl(const AccessRightsElements & elements);

    template <bool with_grant_option>
    void grantImplHelper(const AccessRightsElement & element);

    template <bool grant_option, typename... Args>
    void revokeImpl(const AccessFlags & flags, const Args &... args);

    template <bool grant_option>
    void revokeImpl(const AccessRightsElement & element);

    template <bool grant_option>
    void revokeImpl(const AccessRightsElements & elements);

    template <bool grant_option>
    void revokeImplHelper(const AccessRightsElement & element);

    template <bool grant_option, typename... Args>
    bool isGrantedImpl(const AccessFlags & flags, const Args &... args) const;

    template <bool grant_option>
    bool isGrantedImpl(const AccessRightsElement & element) const;

    template <bool grant_option>
    bool isGrantedImpl(const AccessRightsElements & elements) const;

    template <bool grant_option>
    bool isGrantedImplHelper(const AccessRightsElement & element) const;

    void logTree() const;

    struct Node;
    std::unique_ptr<Node> root;
    std::unique_ptr<Node> root_with_grant_option;
};

}
