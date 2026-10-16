#pragma once

#include <functional>
#include <string>
#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Find parameters in a query and collect them into set.
NameSet analyzeReceiveQueryParams(const std::string & query);

NameSet analyzeReceiveQueryParams(const ASTPtr & ast);

NameToNameMap analyzeReceiveQueryParamsWithType(const ASTPtr & ast);

/// Calls `f` for each member of `ast` that can contain query parameters but is not in `ast.children`.
void forEachQueryParameterMemberOutsideChildren(IAST & ast, const std::function<void(ASTPtr &)> & f);

}
