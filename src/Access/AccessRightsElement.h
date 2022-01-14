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
    bool grant_option = false;
    bool is_partial_revoke = false;

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
    void eraseNonGrantable()
    {
        if (!any_column)
            access_flags &= AccessFlags::allFlagsGrantableOnColumnLevel();
        else if (!any_table)
            access_flags &= AccessFlags::allFlagsGrantableOnTableLevel();
        else if (!any_database)
            access_flags &= AccessFlags::allFlagsGrantableOnDatabaseLevel();
        else
            access_flags &= AccessFlags::allFlagsGrantableOnGlobalLevel();
    }

    bool isEmptyDatabase() const { return !any_database && database.empty(); }

    /// If the database is empty, replaces it with `current_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & current_database)
    {
        if (isEmptyDatabase())
            database = current_database;
    }

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
    String toStringWithoutOptions() const;
};


/// Multiple elements of access rights.
class AccessRightsElements : public std::vector<AccessRightsElement>
{
public:
    bool empty() const { return std::all_of(begin(), end(), [](const AccessRightsElement & e) { return e.empty(); }); }

    bool sameDatabaseAndTable() const
    {
        return (size() < 2) || std::all_of(std::next(begin()), end(), [this](const AccessRightsElement & e) { return e.sameDatabaseAndTable(front()); });
    }

    bool sameOptions() const
    {
        return (size() < 2) || std::all_of(std::next(begin()), end(), [this](const AccessRightsElement & e) { return e.sameOptions(front()); });
    }

    /// Resets flags which cannot be granted.
    void eraseNonGrantable();

    /// If the database is empty, replaces it with `current_database`. Otherwise does nothing.
    void replaceEmptyDatabase(const String & current_database)
    {
        for (auto & element : *this)
            element.replaceEmptyDatabase(current_database);
    }

    /// Returns a human-readable representation like "GRANT SELECT, UPDATE(x, y) ON db.table".
    String toString() const;
    String toStringWithoutOptions() const;
};

}
