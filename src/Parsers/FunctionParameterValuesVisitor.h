#pragma once

#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
struct ScopeAliases;

/// Find parameters in a query parameter values and collect them into map.
struct ParamValuesAnalyzeResult
{
    /// Param name -> resolved param value
    NameToNameMap resolved_param_values;
    /// Param name -> alias
    NameToNameMap unresolved_param_aliases;
};

ParamValuesAnalyzeResult analyzeFunctionParamValues(const ASTPtr & ast, const ContextPtr & context, const ScopeAliases * scope_aliases = nullptr);

}
