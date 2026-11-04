#pragma once

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Replace subcolumns to getSubcolumn() function, keeping the alias or the name of each replaced subcolumn below the root.
void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns);

}

