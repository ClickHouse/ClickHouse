#pragma once

#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
struct ScopeAliases;

/// Find parameters in a query parameter values and collect them into map.
NameToNameMap analyzeFunctionParamValues(const ASTPtr & ast, ContextPtr context, const ScopeAliases * scope_aliases = nullptr);

}
