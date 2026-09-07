#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Whether the expression AST applies `arrayJoin` to the rows it is evaluated on.
///
/// `arrayJoin` is the one function that changes the number of rows, so an expression containing it
/// cannot be used where the result must line up positionally with the input rows (a row policy
/// filter, a TTL expression). It can hide behind an alias - the case-insensitive `unnest`, which is
/// caught by resolving the name to its canonical one instead of comparing the spelling, so the check
/// does not depend on `normalize_function_names` - or behind a SQL UDF that is inlined into the
/// expression later, which is caught by descending into the UDF body. A call inside a nested
/// subquery has its own scope and does not multiply the outer rows, so it is skipped.
bool expressionContainsArrayJoin(const ASTPtr & ast);

}
