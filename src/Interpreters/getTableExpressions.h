#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

struct ASTTableExpression;
class ASTSelectQuery;

using ASTTableExprConstPtrs = std::vector<const ASTTableExpression *>;

NameSet removeDuplicateColumns(NamesAndTypesList & columns);

ASTTableExprConstPtrs getTableExpressions(const ASTSelectQuery & select_query);

const ASTTableExpression * getTableExpression(const ASTSelectQuery & select, size_t table_number);

ASTPtr extractTableExpression(const ASTSelectQuery & select, size_t table_number);

TablesWithColumns getDatabaseAndTablesWithColumns(
    const ASTTableExprConstPtrs & table_expressions, ContextPtr context, bool include_alias_cols, bool include_materialized_cols);

}
