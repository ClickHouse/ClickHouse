#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <optional>


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

    DatabaseAndTableWithAlias() = default;
    DatabaseAndTableWithAlias(const ASTPtr & identifier_node, const String & current_database = "");
    DatabaseAndTableWithAlias(const ASTIdentifier & identifier, const String & current_database = "");
    DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database = "");

    /// "alias." or "table." if alias is empty
    String getQualifiedNamePrefix(bool with_dot = true) const;

    /// Check if it satisfies another db_table name. @note opterion is not symmetric.
    bool satisfies(const DatabaseAndTableWithAlias & table, bool table_may_be_an_alias);
};

struct TableWithColumnNames
{
    DatabaseAndTableWithAlias table;
    Names columns;
    Names hidden_columns;

    TableWithColumnNames(const DatabaseAndTableWithAlias & table_, const Names & columns_)
        : table(table_)
        , columns(columns_)
    {}

    TableWithColumnNames(const DatabaseAndTableWithAlias table_, Names && columns_, Names && hidden_columns_)
        : table(table_)
        , columns(columns_)
        , hidden_columns(hidden_columns_)
    {}

    bool hasColumn(const String & name) const
    {
        if (columns_set.empty())
        {
            columns_set.insert(columns.begin(), columns.end());
            columns_set.insert(hidden_columns.begin(), hidden_columns.end());
        }

        return columns_set.count(name);
    }

private:
    mutable NameSet columns_set;
};

struct TableWithColumnNamesAndTypes
{
    DatabaseAndTableWithAlias table;
    NamesAndTypesList columns;
    NamesAndTypesList hidden_columns;

    TableWithColumnNamesAndTypes(const DatabaseAndTableWithAlias & table_, const NamesAndTypesList & columns_)
        : table(table_)
        , columns(columns_)
    {}

    void addHiddenColumns(const NamesAndTypesList & addition)
    {
        hidden_columns.insert(hidden_columns.end(), addition.begin(), addition.end());
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
};

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database);
std::optional<DatabaseAndTableWithAlias> getDatabaseAndTable(const ASTSelectQuery & select, size_t table_number);

using TablesWithColumnNames = std::vector<TableWithColumnNames>;
using TablesWithColumnNamesAndTypes = std::vector<TableWithColumnNames>;

}
