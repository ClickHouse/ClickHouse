#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Whether the AST subtree contains an `arrayJoin` function call, without descending into
/// nested subqueries (their `arrayJoin` belongs to a different scope). `arrayJoin` changes
/// row cardinality after the source has run, so limit-related optimizations must not truncate
/// its input. The function name is canonicalized so the case-insensitive alias `unnest` is also
/// caught even when function names are not normalized (`normalize_function_names = 0`), and the
/// bodies of SQL user-defined functions are inspected too (with protection from recursive
/// definitions), since a wrapper like `CREATE FUNCTION explode AS a -> arrayJoin(a)` is inlined
/// into the query before execution.
bool astContainsArrayJoinFunction(const ASTPtr & ast);

/// Whether the AST subtree contains a call to a stateful function (`IFunctionBase::isStateful`,
/// e.g. `neighbor`, `runningAccumulate`, `logTrace`), without descending into nested subqueries.
/// Stateful functions give block- and data-order dependent results and side effects, so they must
/// see the same input rows they would see without limit-related optimizations. The bodies of SQL
/// user-defined functions are inspected too (with protection from recursive definitions).
bool astContainsStatefulFunction(const ASTPtr & ast, const ContextPtr & context);

}
