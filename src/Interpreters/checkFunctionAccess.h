#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>


namespace DB
{

/// Enforces `GRANT FUNCTION ON <name>` for the functions listed in
/// `access_control_improvements.functions_requiring_grant`. No-op when that list is empty,
/// which is the default, so the ordinary resolution path pays a single relaxed atomic load.
///
/// Call these ONLY where an identifier is already committed to a real function call.
/// They must never be called from `tryGet`-style helpers: analyzer code uses those as
/// non-throwing capability probes while column and lambda aliases still take priority over
/// function names, so throwing there breaks queries that never invoke the function at all.
///
/// Only ordinary and user defined functions are covered: aggregate names are rejected when the
/// configuration is loaded, so they never reach this check.
void checkFunctionAccess(const ContextPtr & context, const String & function_name);

}
