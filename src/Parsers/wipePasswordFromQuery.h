#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Checks the type of a specified AST and returns true if it can contain a password.
bool canContainPassword(const IAST & ast);

/// Removes a password or its hash from a query if it's specified there or replaces it with some placeholder.
/// This function is used to prepare a query for storing in logs (we don't want logs to contain sensitive information).
/// The function changes only following types of queries:
/// CREATE/ALTER USER.
void wipePasswordFromQuery(ASTPtr ast);

}
