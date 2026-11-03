#pragma once

#include <Common/SQLDefinedHandlers/SQLDefinedHandler.h>
#include <Parsers/IAST_fwd.h>

#include <vector>


namespace DB
{

class ASTCreateHandlerQuery;

/// Build a ready-to-match handler from a CREATE HANDLER AST.
/// Fills in defaults (METHODS -> GET, TYPE -> query), validates the type and the URL regexp,
/// and computes the canonical CREATE HANDLER statement (stored in create_statement).
SQLDefinedHandlerPtr makeSQLDefinedHandler(const ASTCreateHandlerQuery & create);

/// Apply the clauses present in an ALTER HANDLER AST onto a CREATE HANDLER AST (in place).
/// Clauses that are not specified in the ALTER keep their previous values.
void mergeAlterIntoCreateHandler(ASTCreateHandlerQuery & create, const ASTCreateHandlerQuery & alter);

/// The statements a request to a handler with this query executes: `PARALLEL WITH` and `EXECUTE AS <user>
/// <statement>` are looked through (the `EXECUTE AS` wrapper itself is listed too). The pointers refer into the
/// tree of `query`, so a statement can be rewritten in place.
std::vector<ASTPtr *> getExecutedStatements(ASTPtr & query);

/// Whether the statement is a `SELECT` (incl. `UNION` / `INTERSECT` / `EXCEPT`).
bool isSelectShapedStatement(const IAST & statement);

/// Whether the statement is an `INSERT` into a table (not into a table function).
bool isInsertIntoTableStatement(const IAST & statement);

}
