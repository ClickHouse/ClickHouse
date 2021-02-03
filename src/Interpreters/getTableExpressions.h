#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

struct ASTTableExpression;
class ASTSelectQuery;
class Context;

NameSet removeDuplicateColumns(NamesAndTypesList & columns);

std::vector<const ASTTableExpression *> getTableExpressions(const ASTSelectQuery & select_query);
const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number);
ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number);

NamesAndTypesList getColumnsFromTableExpression(const ASTTableExpression & table_expression, const Context & context);
TablesWithColumns getDatabaseAndTablesWithColumns(const std::vector<const ASTTableExpression *> & table_expressions, const Context & context);

}
