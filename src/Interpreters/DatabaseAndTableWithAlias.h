#pragma once

#include <Core/Names.h>
#include <base/types.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <optional>
#include <Core/UUID.h>


namespace DB
{

class ASTSelectQuery;
class ASTTableIdentifier;
struct ASTTableExpression;

/// Extracts database name (and/or alias) from table expression or identifier
struct DatabaseAndTableWithAlias
{
    // TODO(ilezhankin): replace with ASTTableIdentifier
    String database;
    String table;
    String alias;
    UUID uuid = UUIDHelpers::Nil;

    DatabaseAndTableWithAlias() = default;
    explicit DatabaseAndTableWithAlias(const ASTPtr & identifier_node, const String & current_database = "");
    explicit DatabaseAndTableWithAlias(const ASTTableIdentifier & identifier, const String & current_database = "");
    explicit DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database = "");


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

struct TableWithColumnNamesAndTypes
{
    DatabaseAndTableWithAlias table;
    NamesAndTypesList columns;
    NamesAndTypesList hidden_columns; /// Not general columns like MATERIALIZED, ALIAS, VIRTUAL. They are omitted in * and t.* results by default.
    NamesAndTypesList alias_columns;
    NamesAndTypesList materialized_columns;

    TableWithColumnNamesAndTypes(const DatabaseAndTableWithAlias & table_, const NamesAndTypesList & columns_)
        : table(table_)
        , columns(columns_)
    {
        for (auto & col : columns)
            names.insert(col.name);
    }

    bool hasColumn(const String & name) const { return names.contains(name); }

    void addHiddenColumns(const NamesAndTypesList & addition)
    {
        addAdditionalColumns(hidden_columns, addition);
    }

    void addAliasColumns(const NamesAndTypesList & addition)
    {
        addAdditionalColumns(alias_columns, addition);
    }

    void addMaterializedColumns(const NamesAndTypesList & addition)
    {
        addAdditionalColumns(materialized_columns, addition);
    }

private:
    void addAdditionalColumns(NamesAndTypesList & target, const NamesAndTypesList & addition)
    {
        target.insert(target.end(), addition.begin(), addition.end());
        for (const auto & col : addition)
            names.insert(col.name);
    }

    NameSet names;
};

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database);
std::optional<DatabaseAndTableWithAlias> getDatabaseAndTable(const ASTSelectQuery & select, size_t table_number);

using TablesWithColumns = std::vector<TableWithColumnNamesAndTypes>;

}
