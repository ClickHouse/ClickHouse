#pragma once

#include <Access/Common/AccessFlags.h>
#include <tuple>


namespace DB
{
/// An element of access rights which can be represented by single line
/// GRANT ... ON ...
struct AccessRightsElement
{
    AccessFlags access_flags;
    String database;
    String table;
    Strings columns;
    bool any_database = true;
    bool any_table = true;
    bool any_column = true;
    bool grant_option = false;
    bool is_partial_revoke = false;

    AccessRightsElement() = default;
    AccessRightsElement(const AccessRightsElement &) = default;
    AccessRightsElement & operator=(const AccessRightsElement &) = default;
    AccessRightsElement(AccessRightsElement &&) = default;
    AccessRightsElement & operator=(AccessRightsElement &&) = default;

    explicit AccessRightsElement(AccessFlags access_flags_) : access_flags(access_flags_) {}

    AccessRightsElement(AccessFlags access_flags_, std::string_view database_);
    AccessRightsElement(AccessFlags access_flags_, std::string_view database_, std::string_view table_);
    AccessRightsElement(
        AccessFlags access_flags_, std::string_view database_, std::string_view table_, std::string_view column_);

    AccessRightsElement(
        AccessFlags access_flags_,
        std::string_view database_,
        std::string_view table_,
        const std::vector<std::string_view> & columns_);

    AccessRightsElement(
        AccessFlags access_flags_, std::string_view database_, std::string_view table_, const Strings & columns_);

    bool empty() const { return !access_flags || (!any_column && columns.empty()); }

    auto toTuple() const { return std::tie(access_flags, any_database, database, any_table, table, any_column, columns, grant_option, is_partial_revoke); }
    friend bool operator==(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator!=(const AccessRightsElement & left, const AccessRightsElement & right) { return !(left == right); }

    bool sameDatabaseAndTable(const AccessRightsElement & other) const
    {
        return (database == other.database) && (any_database == other.any_database) && (table == other.table)
            && (any_table == other.any_table);
    }

    bool sameOptions(const AccessRightsElement & other) const
    {
        return (grant_option == other.grant_option) && (is_partial_revoke == other.is_partial_revoke);
    }

    /// Resets flags which cannot be granted.
    void eraseNonGrantable();

    bool isEmptyDatabase() const { return !any_database && database.empty(); }

    /// If the database is empty, replaces it with `current_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & current_database);

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
    String toStringWithoutOptions() const;
};


/// Multiple elements of access rights.
class AccessRightsElements : public std::vector<AccessRightsElement>
{
public:
    using Base = std::vector<AccessRightsElement>;
    using Base::Base;

    bool empty() const;
    bool sameDatabaseAndTable() const;
    bool sameOptions() const;

    /// Resets flags which cannot be granted.
    void eraseNonGrantable();

    /// If the database is empty, replaces it with `current_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & current_database);

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
    String toStringWithoutOptions() const;
};

}
