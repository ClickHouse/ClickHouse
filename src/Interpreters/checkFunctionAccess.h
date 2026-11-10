#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>


namespace DB
{

/// Checks `GRANT FUNCTION ON <name>` if the function is listed in
/// `access_control_improvements.functions_requiring_grant`. No-op when that list is empty, which is the default,
/// so the callers pay a single relaxed atomic load. Aggregate function names are rejected when the configuration
/// is loaded, so only ordinary and user defined functions get here.
///
/// The grant is required to create a listed function for a query, so `FunctionFactory` calls this for every
/// function it returns, like the functions which check a privilege of their own when they are created
/// (`demangle`, `getMacro`, ...). That covers every code path which obtains a function, with no call of its own.
///
/// What `FunctionFactory` does not create is checked by name where a call is resolved, in
/// `QueryAnalyzer::resolveFunction`, `ActionsVisitor` and `UserDefinedSQLFunctionVisitor`: user defined functions,
/// and calls which are handled without creating their function.
///
/// The result of a query that calls such a function is not written to the query result cache: a hit is served before
/// the query is analyzed, so it would skip the check after the grant is revoked.
void checkFunctionAccess(const ContextPtr & context, const String & function_name);

}
