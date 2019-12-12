#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

struct ASTTableExpression;
class ASTSelectQuery;
class Context;

std::vector<const ASTTableExpression *> getTableExpressions(const ASTSelectQuery & select_query);
const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number);
ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number);

NamesAndTypesList getColumnsFromTableExpression(const ASTTableExpression & table_expression, const Context & context);
std::vector<TableWithColumnNames> getDatabaseAndTablesWithColumnNames(const ASTSelectQuery & select_query, const Context & context);

}
