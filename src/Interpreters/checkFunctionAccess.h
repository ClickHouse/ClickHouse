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
/// The exception is `FunctionFactory::tryGetWithoutAccessCheck`, for the code which only looks at the properties
/// of a function: there the grant is not required until the function is going to be executed.
///
/// What `FunctionFactory` does not check is checked by name where a call is resolved: user defined functions
/// and calls which are handled without creating their function, in `QueryAnalyzer::resolveFunction`,
/// `ActionsVisitor` and `UserDefinedSQLFunctionVisitor`, and an identifier which turns out to be a call of
/// a function without parentheses (`SELECT currentUser`), in `QueryAnalyzer`.
void checkFunctionAccess(const ContextPtr & context, const String & function_name);

}
