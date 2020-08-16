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

    auto toTuple() const { return std::tie(access_flags, any_database, database, any_table, table, any_column, columns); }
    friend bool operator==(const AccessRightsElement & left, const AccessRightsElement & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator!=(const AccessRightsElement & left, const AccessRightsElement & right) { return !(left == right); }

    bool sameDatabaseAndTable(const AccessRightsElement & other) const
    {
        return (database == other.database) && (any_database == other.any_database) && (table == other.table)
            && (any_table == other.any_table);
    }

    bool isEmptyDatabase() const { return !any_database && database.empty(); }

    /// If the database is empty, replaces it with `new_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & new_database);

    /// Returns a human-readable representation like "SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
};


struct AccessRightsElementWithOptions : public AccessRightsElement
{
    bool grant_option = false;

    enum class Kind
    {
        GRANT,
        REVOKE,
    };
    Kind kind = Kind::GRANT;

    bool sameOptions(const AccessRightsElementWithOptions & other) const
    {
        return (grant_option == other.grant_option) && (kind == other.kind);
    }

    auto toTuple() const { return std::tie(access_flags, any_database, database, any_table, table, any_column, columns, grant_option, kind); }
    friend bool operator==(const AccessRightsElementWithOptions & left, const AccessRightsElementWithOptions & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator!=(const AccessRightsElementWithOptions & left, const AccessRightsElementWithOptions & right) { return !(left == right); }

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
};


/// Multiple elements of access rights.
class AccessRightsElements : public std::vector<AccessRightsElement>
{
public:
    /// Replaces the empty database with `new_database`.
    void replaceEmptyDatabase(const String & new_database);

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
};


class AccessRightsElementsWithOptions : public std::vector<AccessRightsElementWithOptions>
{
public:
    /// Replaces the empty database with `new_database`.
    void replaceEmptyDatabase(const String & new_database);

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
};


inline void AccessRightsElement::replaceEmptyDatabase(const String & new_database)
{
    if (isEmptyDatabase())
        database = new_database;
}

inline void AccessRightsElements::replaceEmptyDatabase(const String & new_database)
{
    for (auto & element : *this)
        element.replaceEmptyDatabase(new_database);
}

inline void AccessRightsElementsWithOptions::replaceEmptyDatabase(const String & new_database)
{
    for (auto & element : *this)
        element.replaceEmptyDatabase(new_database);
}

}
