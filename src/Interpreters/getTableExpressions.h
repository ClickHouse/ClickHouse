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

/// Returns true if the query, or any subquery in its FROM clause (recursively), uses GROUP BY ... WITH TOTALS.
/// Used to decide whether the pipeline must read input to the end so totals are computed over all data.
bool hasWithTotalsInAnySubqueryInFromClause(const ASTSelectQuery & query);

/// Returns true if the AST (a SELECT, UNION or INTERSECT/EXCEPT subtree) contains a branch using
/// LIMIT AFTER/UNTIL together with WITH TOTALS (directly or in a subquery). Such a branch configures
/// LimitRangeTransform to drain its input for totals, so an outer settings LIMIT step must not close
/// the pipeline before that drain completes.
bool rangeBranchNeedsTotalsDrain(const ASTPtr & ast);

/// The parameter is_create_parameterized_view is used in getSampleBlock of the subquery. It is forwarded to getColumnsFromTableExpression.
/// If it is set to true, then query parameters are allowed in the subquery, and that expression is not evaluated.
TablesWithColumns getDatabaseAndTablesWithColumns(
    const ASTTableExprConstPtrs & table_expressions, ContextPtr context, bool include_alias_cols, bool include_materialized_cols, bool is_create_parameterized_view = false);

}
