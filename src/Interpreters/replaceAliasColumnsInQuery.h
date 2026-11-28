#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

class ColumnsDescription;

/// Replace storage alias columns in select query if possible. Return true if the query is changed.
/// @rename_lambda_parameters renames the lambda parameters an expanded definition would be captured by;
/// only for an expression that is evaluated, never stored.
bool replaceAliasColumnsInQuery(
        ASTPtr & ast,
        const ColumnsDescription & columns,
        const NameToNameMap & array_join_result_to_source,
        ContextPtr context,
        const std::unordered_set<IAST *> & excluded_nodes = {},
        bool rename_lambda_parameters = false);

}
