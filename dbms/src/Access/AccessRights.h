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

    /// Revokes everything. It's the same as fullRevoke(AccessType::ALL).
    void clear();

    /// Grants access on a specified database/table/column.
    /// Does nothing if the specified access has been already granted.
    void grant(const AccessFlags & access);
    void grant(const AccessFlags & access, const std::string_view & database);
    void grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table);
    void grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void grant(const AccessRightsElement & access, std::string_view current_database = {});
    void grant(const AccessRightsElements & access, std::string_view current_database = {});

    /// Revokes a specified access granted earlier on a specified database/table/column.
    /// Does nothing if the specified access is not granted.
    /// If the specified access is granted but on upper level (e.g. database for table, table for columns)
    /// or lower level, the function also does nothing.
    /// This function implements the standard SQL REVOKE behaviour.
    void revoke(const AccessFlags & access);
    void revoke(const AccessFlags & access, const std::string_view & database);
    void revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table);
    void revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void revoke(const AccessRightsElement & access, std::string_view current_database = {});
    void revoke(const AccessRightsElements & access, std::string_view current_database = {});

    /// Revokes a specified access granted earlier on a specified database/table/column or on lower levels.
    /// The function also restricts access if it's granted on upper level.
    /// For example, an access could be granted on a database and then revoked on a table in this database.
    /// This function implements the MySQL REVOKE behaviour with partial_revokes is ON.
    void partialRevoke(const AccessFlags & access);
    void partialRevoke(const AccessFlags & access, const std::string_view & database);
    void partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table);
    void partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void partialRevoke(const AccessRightsElement & access, std::string_view current_database = {});
    void partialRevoke(const AccessRightsElements & access, std::string_view current_database = {});

    /// Revokes a specified access granted earlier on a specified database/table/column or on lower levels.
    /// The function also restricts access if it's granted on upper level.
    /// For example, fullRevoke(AccessType::ALL) revokes all grants at all, just like clear();
    /// fullRevoke(AccessType::SELECT, db) means it's not allowed to execute SELECT in that database anymore (from any table).
    void fullRevoke(const AccessFlags & access);
    void fullRevoke(const AccessFlags & access, const std::string_view & database);
    void fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table);
    void fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column);
    void fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns);
    void fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns);
    void fullRevoke(const AccessRightsElement & access, std::string_view current_database = {});
    void fullRevoke(const AccessRightsElements & access, std::string_view current_database = {});

    /// Returns the information about all the access granted.
    struct Elements
    {
        AccessRightsElements grants;
        AccessRightsElements partial_revokes;
    };
    Elements getElements() const;

    /// Returns the information about all the access granted as a string.
    String toString() const;

    /// Whether a specified access granted.
    bool isGranted(const AccessFlags & access) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const;
    bool isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const;
    bool isGranted(const AccessRightsElement & access, std::string_view current_database = {}) const;
    bool isGranted(const AccessRightsElements & access, std::string_view current_database = {}) const;

    friend bool operator ==(const AccessRights & left, const AccessRights & right);
    friend bool operator !=(const AccessRights & left, const AccessRights & right) { return !(left == right); }

    /// Merges two sets of access rights together.
    /// It's used to combine access rights from multiple roles.
    void merge(const AccessRights & other);

private:
    template <typename... Args>
    void grantImpl(const AccessFlags & access, const Args &... args);

    void grantImpl(const AccessRightsElement & access, std::string_view current_database);
    void grantImpl(const AccessRightsElements & access, std::string_view current_database);

    template <int mode, typename... Args>
    void revokeImpl(const AccessFlags & access, const Args &... args);

    template <int mode>
    void revokeImpl(const AccessRightsElement & access, std::string_view current_database);

    template <int mode>
    void revokeImpl(const AccessRightsElements & access, std::string_view current_database);

    template <typename... Args>
    bool isGrantedImpl(const AccessFlags & access, const Args &... args) const;

    bool isGrantedImpl(const AccessRightsElement & access, std::string_view current_database) const;
    bool isGrantedImpl(const AccessRightsElements & access, std::string_view current_database) const;

    template <typename... Args>
    AccessFlags getAccessImpl(const Args &... args) const;

    struct Node;
    std::unique_ptr<Node> root;
};

}
