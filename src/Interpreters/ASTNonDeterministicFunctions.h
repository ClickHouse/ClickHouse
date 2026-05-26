#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Returns true if `ast` contains a function that is not deterministic in the scope of a query
/// (e.g. `rand()`, `now()`), including SQL UDFs (always treated as non-deterministic).
bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context);

}
