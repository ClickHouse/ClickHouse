#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Makes a version of a query without sensitive information (e.g. passwords) for logging.
/// The parameter `parsed query` is allowed to be nullptr if the query cannot be parsed.
/// Does not validate AST, works a best-effort way.
String maskSensitiveInfoInQueryForLogging(const String & query, const ASTPtr & parsed_query, const ContextPtr & context);

/// Makes a version of backup name without sensitive information (e.g. passwords) for logging.
/// Does not validate AST, works a best-effort way.
String maskSensitiveInfoInBackupNameForLogging(const String & backup_name, const ASTPtr & ast, const ContextPtr & context);

}
