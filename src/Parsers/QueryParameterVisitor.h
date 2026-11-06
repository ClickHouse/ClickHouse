#pragma once

#include <string>
#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Find parameters in a query and collect them into set.
NameSet analyzeReceiveQueryParams(const std::string & query);

/// Deferred expressions (row policy filters, masking policy UPDATE / WHERE) are evaluated later,
/// in the context of the query reading the table; a definition check must not see them.
NameSet analyzeReceiveQueryParams(const ASTPtr & ast, bool include_deferred_expressions = true);

NameToNameMap analyzeReceiveQueryParamsWithType(const ASTPtr & ast);

}
