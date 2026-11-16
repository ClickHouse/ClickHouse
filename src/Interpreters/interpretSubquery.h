#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

namespace DB
{

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, size_t subquery_depth, const Names & required_source_columns);

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, const Names & required_source_columns, const SelectQueryOptions & options);

}
