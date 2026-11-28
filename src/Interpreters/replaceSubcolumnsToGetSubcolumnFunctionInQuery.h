#pragma once

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Replace subcolumns to getSubcolumn() function. A replaced subcolumn below the root keeps its alias, or takes its name as one,
/// unless the alias is one of @parents, the columns that `getSubcolumn` reads by name here and in expressions evaluated with this one,
/// or another name in the old analyzer (one alias scope for the whole expression) would then resolve to it.
void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns, const NameSet & parents);

/// Add to @parents the columns whose subcolumns @ast reads.
void collectSubcolumnParents(const IAST & ast, const NamesAndTypesList & columns, NameSet & parents);

}
