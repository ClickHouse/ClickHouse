#pragma once

#include <string>
#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Find parameters in a query and collect them into set.
NameSet analyzeReceiveQueryParams(const std::string & query);

NameSet analyzeReceiveQueryParams(const ASTPtr & ast);

NameToNameMap analyzeReceiveQueryParamsWithType(const ASTPtr & ast);

}
