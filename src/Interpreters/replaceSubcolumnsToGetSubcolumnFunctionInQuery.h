#pragma once

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Replace subcolumns to getSubcolumn() function. A replaced subcolumn below the root keeps its alias, or takes its name as one,
/// unless the old analyzer, which has one alias scope for the whole expression, would then resolve a name differently.
void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns);

}

