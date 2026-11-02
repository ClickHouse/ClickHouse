#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>


namespace DB
{

/// Enforces `GRANT FUNCTION ON <name>` for the functions listed in
/// `access_control_improvements.functions_requiring_grant`. No-op when that list is empty,
/// which is the default, so the ordinary resolution path pays a single relaxed atomic load.
/// The result of a query that calls such a function is not written to the query result cache.
///
/// Call these ONLY where an identifier is already committed to a real function call.
/// They must never be called from `tryGet`-style helpers: analyzer code uses those as
/// non-throwing capability probes while column and lambda aliases still take priority over
/// function names, so throwing there breaks queries that never invoke the function at all.
///
/// Only ordinary and user defined functions are covered: aggregate names are rejected when the
/// configuration is loaded, so they never reach this check.
void checkFunctionAccess(const ContextPtr & context, const String & function_name);

/// The same for every function call in the expression `ast`, as written. For an expression analyzed by the legacy
/// `ExpressionAnalyzer`: its `TreeRewriter` renames some calls (`in` to `nullIn` under `transform_null_in`, ...)
/// before `ActionsVisitor` checks them. Only for a standalone expression, not for a query with table functions.
void checkFunctionAccessInExpression(const ContextPtr & context, const ASTPtr & ast);

}
