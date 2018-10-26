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


struct DatabaseAndTableWithAlias
{
    String database;
    String table;
    String alias;

    /// "alias." or "database.table." if alias is empty
    String getQualifiedNamePrefix() const;

    /// If ast is ASTIdentifier, prepend getQualifiedNamePrefix() to it's name.
    void makeQualifiedName(const ASTPtr & ast) const;
};

void stripIdentifier(DB::ASTPtr & ast, size_t num_qualifiers_to_strip);

DatabaseAndTableWithAlias getTableNameWithAliasFromTableExpression(const ASTTableExpression & table_expression,
                                                                   const String & current_database);

size_t getNumComponentsToStripInOrderToTranslateQualifiedName(const ASTIdentifier & identifier,
                                                              const DatabaseAndTableWithAlias & names);

std::pair<String, String> getDatabaseAndTableNameFromIdentifier(const ASTIdentifier & identifier);

std::vector<const ASTTableExpression *> getSelectTablesExpression(const ASTSelectQuery * select_query);
std::vector<DatabaseAndTableWithAlias> getDatabaseAndTableWithAliases(const ASTSelectQuery * select_query, const String & current_database);

}
