#pragma once

#include <Core/Types.h>
#include <Access/AccessRightsElement.h>
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
    AccessRights(const AccessFlags & access);
    ~AccessRights();
    AccessRights(const AccessRights & src);
    AccessRights & operator =(const AccessRights & src);
    AccessRights(AccessRights && src);
    AccessRights & operator =(AccessRights && src);

    bool isEmpty() const;

    /// Revokes everything. It's the same as revoke(AccessType::ALL).
    void clear();

    /// Grants access on a specified database/table/column.
    /// Does nothing if the specified access has been already granted.
    void grant(const AccessFlags & flags);
    void grant(const AccessFlags & flags, const std::string_view & database);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void grant(const AccessRightsElement & element, std::string_view current_database = {});
    void grant(const AccessRightsElements & elements, std::string_view current_database = {});

    /// Revokes a specified access granted earlier on a specified database/table/column.
    /// For example, revoke(AccessType::ALL) revokes all grants at all, just like clear();
    void revoke(const AccessFlags & flags);
    void revoke(const AccessFlags & flags, const std::string_view & database);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void revoke(const AccessRightsElement & element, std::string_view current_database = {});
    void revoke(const AccessRightsElements & elements, std::string_view current_database = {});

    /// Returns the information about all the access granted.
    struct GrantsAndPartialRevokes
    {
        AccessRightsElements grants;
        AccessRightsElements revokes;
    };
    AccessRightsElements getGrants() const;
    AccessRightsElements getPartialRevokes() const;
    GrantsAndPartialRevokes getGrantsAndPartialRevokes() const;

    /// Returns the information about all the access granted as a string.
    String toString() const;

    /// Whether a specified access granted.
    bool isGranted(const AccessFlags & flags) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(const AccessRightsElement & element, std::string_view current_database = {}) const;
    bool isGranted(const AccessRightsElements & elements, std::string_view current_database = {}) const;

    friend bool operator ==(const AccessRights & left, const AccessRights & right);
    friend bool operator !=(const AccessRights & left, const AccessRights & right) { return !(left == right); }

    /// Merges two sets of access rights together.
    /// It's used to combine access rights from multiple roles.
    void merge(const AccessRights & other);

private:
    template <typename... Args>
    void grantImpl(const AccessFlags & flags, const Args &... args);

    template <typename... Args>
    void revokeImpl(const AccessFlags & flags, const Args &... args);

    template <typename... Args>
    bool isGrantedImpl(const AccessFlags & flags, const Args &... args) const;

    bool isGrantedImpl(const AccessRightsElement & element, std::string_view current_database) const;
    bool isGrantedImpl(const AccessRightsElements & elements, std::string_view current_database) const;

    template <typename... Args>
    AccessFlags getAccessImpl(const Args &... args) const;

    void getGrantsAndPartialRevokesImpl(AccessRightsElements * grants, AccessRightsElements * partial_revokes) const;

    void logTree() const;

    struct Node;
    std::unique_ptr<Node> root;
};

}
