#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

namespace DB
{

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, size_t subquery_depth, const Names & required_source_columns);

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, const Names & required_source_columns, const SelectQueryOptions & options);

/** The same subquery, analyzed by the analyzer.
  * A table expression that is not a subquery - a table name or a table function - becomes `SELECT <columns> FROM it`,
  * which is what the analyzer expects, and the restrictions on the size of the result of a whole query are lifted,
  * because the result of this one is not the result of the whole query.
  */
std::shared_ptr<InterpreterSelectQueryAnalyzer> interpretSubqueryWithAnalyzer(
    const ASTPtr & table_expression, ContextPtr context, size_t subquery_depth, const Names & required_source_columns);

}
