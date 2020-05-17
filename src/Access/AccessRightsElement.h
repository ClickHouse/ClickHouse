#pragma once

#include <Access/AccessFlags.h>


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

    AccessRightsElement() = default;
    AccessRightsElement(const AccessRightsElement &) = default;
    AccessRightsElement & operator=(const AccessRightsElement &) = default;
    AccessRightsElement(AccessRightsElement &&) = default;
    AccessRightsElement & operator=(AccessRightsElement &&) = default;

    AccessRightsElement(AccessFlags access_flags_) : access_flags(access_flags_) {}

    AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_)
        : access_flags(access_flags_), database(database_), any_database(false)
    {
    }

    AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_)
        : access_flags(access_flags_), database(database_), table(table_), any_database(false), any_table(false)
    {
    }

    AccessRightsElement(
        AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_, const std::string_view & column_)
        : access_flags(access_flags_)
        , database(database_)
        , table(table_)
        , columns({String{column_}})
        , any_database(false)
        , any_table(false)
        , any_column(false)
    {
    }

    AccessRightsElement(
        AccessFlags access_flags_,
        const std::string_view & database_,
        const std::string_view & table_,
        const std::vector<std::string_view> & columns_)
        : access_flags(access_flags_), database(database_), table(table_), any_database(false), any_table(false), any_column(false)
    {
        columns.resize(columns_.size());
        for (size_t i = 0; i != columns_.size(); ++i)
            columns[i] = String{columns_[i]};
    }

    AccessRightsElement(
        AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_, const Strings & columns_)
        : access_flags(access_flags_)
        , database(database_)
        , table(table_)
        , columns(columns_)
        , any_database(false)
        , any_table(false)
        , any_column(false)
    {
    }

    auto toTuple() const { return std::tie(access_flags, database, any_database, table, any_table, columns, any_column); }
    friend bool operator==(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator!=(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() != right.toTuple(); }
    friend bool operator<(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() < right.toTuple(); }
    friend bool operator>(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() > right.toTuple(); }
    friend bool operator<=(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() <= right.toTuple(); }
    friend bool operator>=(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() >= right.toTuple(); }

    /// Sets the database.
    void setDatabase(const String & new_database);

    /// If the database is empty, replaces it with `new_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & new_database);

    bool isEmptyDatabase() const;

    /// Returns a human-readable representation like "SELECT, UPDATE(x, y) ON db.table".
    /// The returned string isn't prefixed with the "GRANT" keyword.
    String toString() const;
    String toStringWithoutON() const;
};


/// Multiple elements of access rights.
class AccessRightsElements : public std::vector<AccessRightsElement>
{
public:
    /// Replaces the empty database with `new_database`.
    void replaceEmptyDatabase(const String & new_database);

    /// Returns a human-readable representation like "SELECT, UPDATE(x, y) ON db.table".
    /// The returned string isn't prefixed with the "GRANT" keyword.
    String toString() const { return AccessRightsElements(*this).toString(); }
    String toString();

    /// Reorder and group elements to show them in more readable form.
    void normalize();
};

}
