#pragma once

#include <base/types.h>
#include <Access/Common/AccessRightsElement.h>
#include <IO/WriteBuffer.h>

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
    explicit AccessRights(const AccessRightsElement & element);
    explicit AccessRights(const AccessRightsElements & elements);

    ~AccessRights();
    AccessRights(const AccessRights & src);
    AccessRights & operator =(const AccessRights & src);
    AccessRights(AccessRights && src) noexcept;
    AccessRights & operator =(AccessRights && src) noexcept;

    [[nodiscard]] bool isEmpty() const;

    /// Revokes everything. It's the same as revoke(AccessType::ALL).
    void clear();

    /// Returns the information about all the access granted as a string.
    [[nodiscard]] String toString() const;

    /// Returns the information about all the access granted.
    [[nodiscard]] AccessRightsElements getElements() const;

    /// Returns the subset of access rights that can be granted to others
    /// (i.e. rights with GRANT OPTION, with partial revokes applied).
    [[nodiscard]] AccessRights getGrantableRights() const;

    /// Grants access on a specified database/table/column.
    /// Does nothing if the specified access has been already granted.
    void grant(const AccessFlags & flags);
    void grant(const AccessFlags & flags, std::string_view database);
    void grant(const AccessFlags & flags, std::string_view database, std::string_view table);
    void grant(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column);
    void grant(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns);
    void grant(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns);
    void grant(const AccessRightsElement & element);
    void grant(const AccessRightsElements & elements);

    void grantWildcard(const AccessFlags & flags);
    void grantWildcard(const AccessFlags & flags, std::string_view database);
    void grantWildcard(const AccessFlags & flags, std::string_view database, std::string_view table);
    void grantWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column);
    void grantWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns);
    void grantWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns);

    void grantWithGrantOption(const AccessFlags & flags);
    void grantWithGrantOption(const AccessFlags & flags, std::string_view database);
    void grantWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table);
    void grantWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column);
    void grantWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns);
    void grantWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns);
    void grantWithGrantOption(const AccessRightsElement & element);
    void grantWithGrantOption(const AccessRightsElements & elements);

    void grantWildcardWithGrantOption(const AccessFlags & flags);
    void grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database);
    void grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table);
    void grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column);
    void grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns);
    void grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns);

    /// Revokes a specified access granted earlier on a specified database/table/column.
    /// For example, revoke(AccessType::ALL) revokes all grants at all, just like clear();
    void revoke(const AccessFlags & flags);
    void revoke(const AccessFlags & flags, std::string_view database);
    void revoke(const AccessFlags & flags, std::string_view database, std::string_view table);
    void revoke(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column);
    void revoke(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns);
    void revoke(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns);
    void revoke(const AccessRightsElement & element);
    void revoke(const AccessRightsElements & elements);

    void revokeWildcard(const AccessFlags & flags);
    void revokeWildcard(const AccessFlags & flags, std::string_view database);
    void revokeWildcard(const AccessFlags & flags, std::string_view database, std::string_view table);
    void revokeWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column);
    void revokeWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns);
    void revokeWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns);

    void revokeGrantOption(const AccessFlags & flags);
    void revokeGrantOption(const AccessFlags & flags, std::string_view database);
    void revokeGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table);
    void revokeGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column);
    void revokeGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns);
    void revokeGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns);
    void revokeGrantOption(const AccessRightsElement & element);
    void revokeGrantOption(const AccessRightsElements & elements);

    void revokeWildcardGrantOption(const AccessFlags & flags);
    void revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database);
    void revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table);
    void revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column);
    void revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns);
    void revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns);

    /// Whether a specified access granted.
    [[nodiscard]] bool isGranted(const AccessFlags & flags) const;
    [[nodiscard]] bool isGranted(const AccessFlags & flags, std::string_view database) const;
    [[nodiscard]] bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    [[nodiscard]] bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    [[nodiscard]] bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    [[nodiscard]] bool isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    [[nodiscard]] bool isGranted(const AccessRightsElement & element) const;
    [[nodiscard]] bool isGranted(const AccessRightsElements & elements) const;

    [[nodiscard]] bool isGrantedWildcard(const AccessFlags & flags) const;
    [[nodiscard]] bool isGrantedWildcard(const AccessFlags & flags, std::string_view database) const;
    [[nodiscard]] bool isGrantedWildcard(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    [[nodiscard]] bool isGrantedWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    [[nodiscard]] bool isGrantedWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    [[nodiscard]] bool isGrantedWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;

    [[nodiscard]] bool hasGrantOption(const AccessFlags & flags) const;
    [[nodiscard]] bool hasGrantOption(const AccessFlags & flags, std::string_view database) const;
    [[nodiscard]] bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    [[nodiscard]] bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    [[nodiscard]] bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    [[nodiscard]] bool hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;
    [[nodiscard]] bool hasGrantOption(const AccessRightsElement & element) const;
    [[nodiscard]] bool hasGrantOption(const AccessRightsElements & elements) const;

    [[nodiscard]] bool hasGrantOptionWildcard(const AccessFlags & flags) const;
    [[nodiscard]] bool hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database) const;
    [[nodiscard]] bool hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database, std::string_view table) const;
    [[nodiscard]] bool hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const;
    [[nodiscard]] bool hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const;
    [[nodiscard]] bool hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const;

    /// Checks if a given `access_rights` is a subset for the current access rights.
    /// This function checks both access rights with and without grant option.
    ///
    /// root ⊇ other.root && root_with_grant_option ⊇ other.root_with_grant_option
    [[nodiscard]] bool contains(const AccessRights & access_rights) const;

    /// Similar to `contains`, but checks that current access rights with grant option contain other access rights
    ///
    /// root_with_grant_option ⊇ other.root
    [[nodiscard]] bool containsWithGrantOption(const AccessRights & access_rights) const;

    /// Merges two sets of access rights together.
    /// It's used to combine access rights from multiple roles.
    void makeUnion(const AccessRights & other);

    /// Makes an intersection of access rights.
    void makeIntersection(const AccessRights & other);

    /// Makes a difference (relative complement) of access rights.
    void makeDifference(const AccessRights & other);

    /// Returns filters for specific parameter
    /// S3('s3://url.*')
    struct Filter
    {
        AccessFlags access_flags;
        String path;
    };
    [[nodiscard]] std::vector<Filter> getFilters(std::string_view parameter) const;

    /// Traverse the tree and modify each access flags.
    using ModifyFlagsFunction = std::function<AccessFlags(
        const AccessFlags & flags,
        const AccessFlags & min_flags_with_children,
        const AccessFlags & max_flags_with_children,
        const size_t level,
        bool grant_option,
        bool leaf_or_wildcard)>;
    void modifyFlags(const ModifyFlagsFunction & function);

    friend bool operator ==(const AccessRights & left, const AccessRights & right);
    friend bool operator !=(const AccessRights & left, const AccessRights & right) { return !(left == right); }

    /// Makes full access rights (GRANT ALL ON *.* WITH GRANT OPTION).
    static AccessRights getFullAccess();

    /// Methods for tests
    void dumpTree(WriteBuffer & buffer) const;
    [[nodiscard]] std::vector<String> dumpNodes() const;

private:
    template <bool with_grant_option, bool wildcard, typename... Args>
    void grantImpl(const AccessFlags & flags, const Args &... args);

    template <bool with_grant_option, bool wildcard>
    void grantImpl(const AccessRightsElement & element);

    template <bool with_grant_option, bool wildcard>
    void grantImpl(const AccessRightsElements & elements);

    template <bool with_grant_option, bool wildcard>
    void grantImplHelper(const AccessRightsElement & element);

    template <bool grant_option, bool wildcard, typename... Args>
    void revokeImpl(const AccessFlags & flags, const Args &... args);

    template <bool grant_option, bool wildcard>
    void revokeImpl(const AccessRightsElement & element);

    template <bool grant_option, bool wildcard>
    void revokeImpl(const AccessRightsElements & elements);

    template <bool grant_option, bool wildcard>
    void revokeImplHelper(const AccessRightsElement & element);

    template <bool grant_option, bool wildcard, typename... Args>
    bool isGrantedImpl(const AccessFlags & flags, const Args &... args) const;

    template <bool grant_option, bool wildcard>
    [[nodiscard]] bool isGrantedImpl(const AccessRightsElement & element) const;

    template <bool grant_option, bool wildcard>
    [[nodiscard]] bool isGrantedImpl(const AccessRightsElements & elements) const;

    template <bool grant_option>
    [[nodiscard]] bool containsImpl(const AccessRights & other) const;

    template <bool grant_option, bool wildcard>
    [[nodiscard]] bool isGrantedImplHelper(const AccessRightsElement & element) const;

    struct Node;
    std::unique_ptr<Node> root;
    std::unique_ptr<Node> root_with_grant_option;
};

}
