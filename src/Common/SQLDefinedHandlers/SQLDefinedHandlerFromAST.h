#pragma once

#include <Common/SQLDefinedHandlers/SQLDefinedHandler.h>


namespace DB
{

class ASTCreateHandlerQuery;
class IAST;

/// Whether the query itself reads the HTTP request body as its data: a plain `INSERT`, or an
/// `INSERT ... SELECT` reading from the `input` table function. Used both for SQL-defined handlers and for the
/// config-defined `predefined_query_handler`, whose query is equally known in advance, so that the HTTP layer
/// knows whether an unframed body-carrying request can be accepted (see `HTTPHandler::handleRequest`).
bool queryConsumesRequestBody(const IAST & query);

/// Build a ready-to-match handler from a CREATE HANDLER AST.
/// Fills in defaults (METHODS -> GET, TYPE -> query), validates the type and the URL regexp,
/// and computes the canonical CREATE HANDLER statement (stored in create_statement).
SQLDefinedHandlerPtr makeSQLDefinedHandler(const ASTCreateHandlerQuery & create);

/// Apply the clauses present in an ALTER HANDLER AST onto a CREATE HANDLER AST (in place).
/// Clauses that are not specified in the ALTER keep their previous values.
void mergeAlterIntoCreateHandler(ASTCreateHandlerQuery & create, const ASTCreateHandlerQuery & alter);

}
