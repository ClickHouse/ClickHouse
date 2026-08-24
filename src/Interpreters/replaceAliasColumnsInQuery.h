#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

class ColumnsDescription;

/// Replace storage alias columns in select query if possible. Return true if the query is changed.
bool replaceAliasColumnsInQuery(
        ASTPtr & ast,
        const ColumnsDescription & columns,
        const NameToNameMap & array_join_result_to_source,
        ContextPtr context,
        const std::unordered_set<IAST *> & excluded_nodes = {});

}
