#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>


namespace DB
{

/// `GRANT FUNCTION ON <name>` for the functions listed in `access_control_improvements.functions_requiring_grant`.
/// No-op when that list is empty, which is the default, so the ordinary resolution path pays a single relaxed
/// atomic load. Only ordinary and user defined functions are covered: aggregate names are rejected when the
/// configuration is loaded, so they never reach the check.
///
/// A call is checked on two levels:
/// - As written, before anything can rename or replace it: `QueryAnalyzer::resolveFunction` and the identifiers it
///   resolves as functions without parentheses; on the legacy path `TreeRewriter::normalize` and `ActionsVisitor`.
///   Where a written name disappears before the analysis, it is checked there: in `normalizeDivision`,
///   `replaceLegacyToTime` and `UserDefinedSQLFunctionVisitor`, which inlines SQL UDFs.
/// - As executed, by the name of the implementation which runs: `QueryAnalyzer::resolveFunction` before the constant
///   folding, `CheckFunctionAccessPass` after the optimizations of the query tree; on the legacy path `ActionsVisitor`;
///   in a query plan received from a client, which skips the analysis, `ActionsDAG::deserialize`.
///
/// Call these ONLY where an identifier is already committed to a real function call. They must never be called from
/// `tryGet`-style helpers: analyzer code uses those as non-throwing capability probes while column and lambda aliases
/// still take priority over function names, so throwing there breaks queries that never invoke the function at all.
///
/// The result of a query that calls such a function is not written to the query result cache: a hit is served before
/// the query is analyzed, so it would skip the check after the grant is revoked.
void checkFunctionAccess(const ContextPtr & context, const String & function_name);

/// The same for every call in `ast`, as written. The name of a table function and the parameter list of a lambda,
/// `tuple(x)`, are not calls.
void checkFunctionAccessInAST(const ContextPtr & context, const ASTPtr & ast);

}
