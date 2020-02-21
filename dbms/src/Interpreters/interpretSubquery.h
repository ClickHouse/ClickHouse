#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

namespace DB
{

class Context;

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, const Context & context, size_t subquery_depth, const Names & required_source_columns);

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, const Context & context, const Names & required_source_columns, const SelectQueryOptions & options);

}
