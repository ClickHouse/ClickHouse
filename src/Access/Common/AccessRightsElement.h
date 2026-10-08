#pragma once

#include <Access/Common/AccessFlags.h>
#include <IO/WriteBuffer.h>
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
    String parameter;
    String filter;

    bool wildcard = false;
    bool default_database = false;

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

    bool empty() const { return !access_flags || (!anyColumn() && columns.empty()); }

    bool anyDatabase() const { return database.empty() && table.empty() && !default_database; }
    bool anyTable() const { return table.empty(); }
    bool anyColumn() const { return columns.empty(); }
    bool anyParameter() const { return parameter.empty(); }
    bool hasFilter() const { return !filter.empty(); }

    auto toTuple() const { return std::tie(access_flags, default_database, database, table, columns, parameter, wildcard, grant_option, is_partial_revoke); }
    friend bool operator==(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator!=(const AccessRightsElement & left, const AccessRightsElement & right) { return !(left == right); }

    bool sameDatabaseAndTableAndParameter(const AccessRightsElement & other) const
    {
        return sameDatabaseAndTable(other) && sameParameter(other) && (wildcard == other.wildcard) && (filter == other.filter);
    }

    bool sameParameter(const AccessRightsElement & other) const
    {
        return (parameter == other.parameter) && (anyParameter() == other.anyParameter())
            && (access_flags.getParameterType() == other.access_flags.getParameterType())
            && (isGlobalWithParameter() == other.isGlobalWithParameter());
    }

    bool sameDatabaseAndTable(const AccessRightsElement & other) const
    {
        return (database == other.database) && (table == other.table) && (anyDatabase() == other.anyDatabase()) && (anyTable() == other.anyTable());
    }

    bool sameOptions(const AccessRightsElement & other) const
    {
        return (grant_option == other.grant_option) && (is_partial_revoke == other.is_partial_revoke);
    }

    /// Returns only those flags which can be granted.
    AccessFlags getGrantableFlags() const;

    /// Throws an exception if some flags can't be granted.
    void throwIfNotGrantable() const;

    /// Resets flags which cannot be granted.
    void eraseNotGrantable();

    bool isEmptyDatabase() const { return database.empty() and !anyDatabase(); }

    /// If the database is empty, replaces it with `current_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & current_database);

    /// Checks if the current access type is deprecated and replaces it with the correct one.
    void replaceDeprecated();

    void makeBackwardCompatible();

    bool isGlobalWithParameter() const { return access_flags.isGlobalWithParameter(); }

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
    String toStringWithoutOptions() const;

    void formatColumnNames(WriteBuffer & buffer) const;
    void formatFilter(WriteBuffer & buffer) const;
    void formatONClause(WriteBuffer & buffer) const;
};


/// Multiple elements of access rights.
class AccessRightsElements : public std::vector<AccessRightsElement>
{
public:
    using Base = std::vector<AccessRightsElement>;
    using Base::Base;

    bool empty() const;
    bool sameDatabaseAndTableAndParameter() const;
    bool sameDatabaseAndTable() const;
    bool sameOptions() const;

    /// Throws an exception if some flags can't be granted.
    void throwIfNotGrantable() const;

    /// Resets flags which cannot be granted.
    void eraseNotGrantable();

    /// For each element checks if the current access type is deprecated and replaces it with the correct one.
    void replaceDeprecated();

    /// If the database is empty, replaces it with `current_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & current_database);

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
    String toStringWithoutOptions() const;
    void formatElementsWithoutOptions(WriteBuffer & buffer) const;
};

}
