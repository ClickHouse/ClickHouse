#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTQQuery;

/// Compiles a program of the `q` dialect into a ClickHouse query.
///
/// The statements of the program are compiled one after another: an assignment binds a name for the
/// following statements (a table becomes a subquery, a lambda is inlined where it is applied), and
/// the value of the last statement becomes the result of the query.
ASTPtr compileQProgram(const ASTQQuery & query, ContextPtr context);

}
