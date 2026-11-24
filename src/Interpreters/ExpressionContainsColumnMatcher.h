#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// The first column matcher (`*`, `t.*`, `COLUMNS(...)`, `t.COLUMNS(...)`) in the expression AST, or nullptr.
///
/// A matcher expands into the columns of the query's table sources, so it has no meaning in an expression that
/// is evaluated over a single table on its own (a row policy, `additional_table_filters`). It can hide behind a
/// SQL UDF that is inlined into the expression later, which is caught by descending into the UDF body. A matcher
/// inside a nested subquery resolves against that subquery's own tables, so subqueries are skipped.
const IAST * findColumnMatcherInExpression(const IAST & ast);

}
