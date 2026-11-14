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

/// The statements a request to the handler executes, looking through `PARALLEL WITH` and `EXECUTE AS <user> <statement>`
/// (the `EXECUTE AS` itself is listed too). The pointers refer into `query`, so a statement can be rewritten in place.
std::vector<ASTPtr *> getExecutedStatements(ASTPtr & query);

bool isSelectShapedStatement(const IAST & statement);

bool isInsertIntoTableStatement(const IAST & statement);

}
