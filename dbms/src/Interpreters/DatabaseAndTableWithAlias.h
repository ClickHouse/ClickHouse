#pragma once

#include <memory>
#include <Core/Types.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class ASTSelectQuery;
class ASTIdentifier;
struct ASTTableExpression;


/// Extracts database name (and/or alias) from table expression or identifier
struct DatabaseAndTableWithAlias
{
    String database;
    String table;
    String alias;

    DatabaseAndTableWithAlias(const ASTIdentifier & identifier, const String & current_database = "");
    DatabaseAndTableWithAlias(const ASTTableExpression & table_expression, const String & current_database);

    /// "alias." or "database.table." if alias is empty
    String getQualifiedNamePrefix() const;

    /// If ast is ASTIdentifier, prepend getQualifiedNamePrefix() to it's name.
    void makeQualifiedName(const ASTPtr & ast) const;
};

void stripIdentifier(DB::ASTPtr & ast, size_t num_qualifiers_to_strip);

size_t getNumComponentsToStripInOrderToTranslateQualifiedName(const ASTIdentifier & identifier,
                                                              const DatabaseAndTableWithAlias & names);

std::vector<DatabaseAndTableWithAlias> getDatabaseAndTables(const ASTSelectQuery & select_query, const String & current_database);
std::shared_ptr<DatabaseAndTableWithAlias> getDatabaseAndTable(const ASTSelectQuery & select, size_t table_number);

std::vector<const ASTTableExpression *> getSelectTablesExpression(const ASTSelectQuery & select_query);
ASTPtr getTableFunctionOrSubquery(const ASTSelectQuery & select, size_t table_number);

}
