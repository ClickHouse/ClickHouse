#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
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

using TableWithColumnNames = std::pair<DatabaseAndTableWithAlias, Names>;

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database);
std::optional<DatabaseAndTableWithAlias> getDatabaseAndTable(const ASTSelectQuery & select, size_t table_number);

std::vector<TableWithColumnNames> getDatabaseAndTablesWithColumnNames(const ASTSelectQuery & select_query, const Context & context);

std::vector<const ASTTableExpression *> getSelectTablesExpression(const ASTSelectQuery & select_query);
ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number);

}
