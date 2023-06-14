#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

namespace DB
{

class InterpreterSelectQueryAnalyzer;

std::shared_ptr<InterpreterSelectQueryAnalyzer> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, size_t subquery_depth);

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, size_t subquery_depth, const Names & required_source_columns);

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, const Names & required_source_columns, const SelectQueryOptions & options);

}
