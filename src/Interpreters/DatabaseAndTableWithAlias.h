#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <optional>
#include <Core/UUID.h>


namespace DB
{

class ASTSelectQuery;
class ASTIdentifier;
struct ASTTableExpression;
class Context;


/// Extracts database name (and/or alias) from table expression or identifier
struct DatabaseAndTableWithAlias
{
    String database;
    String table;
    String alias;
    UUID uuid = UUIDHelpers::Nil;

    DatabaseAndTableWithAlias() = default;
    DatabaseAndTableWithAlias(const ASTPtr & identifier_node, const String & current_database = "");
    DatabaseAndTableWithAlias(const ASTIdentifier & identifier, const String & current_database = "");
    DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database = "");

    /// "alias." or "table." if alias is empty
    String getQualifiedNamePrefix(bool with_dot = true) const;

    /// Check if it satisfies another db_table name. @note opterion is not symmetric.
    bool satisfies(const DatabaseAndTableWithAlias & table, bool table_may_be_an_alias) const;

    /// Exactly the same table name
    bool same(const DatabaseAndTableWithAlias & db_table) const
    {
        return database == db_table.database && table == db_table.table && alias == db_table.alias && uuid == db_table.uuid;
    }
};

struct TableWithColumnNames
{
    DatabaseAndTableWithAlias table;
    Names columns;
    Names hidden_columns; /// Not general columns like MATERIALIZED and ALIAS. They are omitted in * and t.* results.

    TableWithColumnNames(const DatabaseAndTableWithAlias & table_, const Names & columns_)
        : table(table_)
        , columns(columns_)
    {
        columns_set.insert(columns.begin(), columns.end());
    }

    TableWithColumnNames(const DatabaseAndTableWithAlias table_, Names && columns_, Names && hidden_columns_)
        : table(table_)
        , columns(columns_)
        , hidden_columns(hidden_columns_)
    {
        columns_set.insert(columns.begin(), columns.end());
        columns_set.insert(hidden_columns.begin(), hidden_columns.end());
    }

    bool hasColumn(const String & name) const { return columns_set.count(name); }

private:
    NameSet columns_set;
};

struct TableWithColumnNamesAndTypes
{
    DatabaseAndTableWithAlias table;
    NamesAndTypesList columns;
    NamesAndTypesList hidden_columns; /// Not general columns like MATERIALIZED and ALIAS. They are omitted in * and t.* results.

    TableWithColumnNamesAndTypes(const DatabaseAndTableWithAlias & table_, const NamesAndTypesList & columns_)
        : table(table_)
        , columns(columns_)
    {
        for (auto & col : columns)
            names.insert(col.name);
    }

    bool hasColumn(const String & name) const { return names.count(name); }

    void addHiddenColumns(const NamesAndTypesList & addition)
    {
        hidden_columns.insert(hidden_columns.end(), addition.begin(), addition.end());
        for (auto & col : addition)
            names.insert(col.name);
    }

    TableWithColumnNames removeTypes() const
    {
        Names out_columns;
        out_columns.reserve(columns.size());
        for (auto & col : columns)
            out_columns.push_back(col.name);

        Names out_hidden_columns;
        out_hidden_columns.reserve(hidden_columns.size());
        for (auto & col : hidden_columns)
            out_hidden_columns.push_back(col.name);

        return TableWithColumnNames(table, std::move(out_columns), std::move(out_hidden_columns));
    }

private:
    NameSet names;
};

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database);
std::optional<DatabaseAndTableWithAlias> getDatabaseAndTable(const ASTSelectQuery & select, size_t table_number);

using TablesWithColumnNames = std::vector<TableWithColumnNames>;
using TablesWithColumnNamesAndTypes = std::vector<TableWithColumnNames>;

}
