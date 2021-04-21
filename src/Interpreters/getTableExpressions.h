#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

struct ASTTableExpression;
class ASTSelectQuery;

NameSet removeDuplicateColumns(NamesAndTypesList & columns);

std::vector<const ASTTableExpression *> getTableExpressions(const ASTSelectQuery & select_query);
const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number);
ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number);

NamesAndTypesList getColumnsFromTableExpression(const ASTTableExpression & table_expression, ContextPtr context);
TablesWithColumns getDatabaseAndTablesWithColumns(const std::vector<const ASTTableExpression *> & table_expressions, ContextPtr context);

}
