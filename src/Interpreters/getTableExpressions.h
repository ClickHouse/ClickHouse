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

/// Returns true if the query itself, or its first table expression (recursing through the branches of
/// `UNION`, `INTERSECT` and `EXCEPT`), uses `GROUP BY ... WITH TOTALS`. Later table expressions are join
/// right sides, which every totals-capable join algorithm reads to the end before it produces a row, so
/// only the first one can have its totals cut short by a limit above it. Used to decide whether the
/// pipeline must read its input to the end so that the totals are computed over all data.
bool hasWithTotalsInAnySubqueryInFromClause(const ASTSelectQuery & query);

/// The parameter is_create_parameterized_view is used in getSampleBlock of the subquery. It is forwarded to getColumnsFromTableExpression.
/// If it is set to true, then query parameters are allowed in the subquery, and that expression is not evaluated.
TablesWithColumns getDatabaseAndTablesWithColumns(
    const ASTTableExprConstPtrs & table_expressions, ContextPtr context, bool include_alias_cols, bool include_materialized_cols, bool is_create_parameterized_view = false);

}
