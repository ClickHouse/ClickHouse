#pragma once

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Replace subcolumns to getSubcolumn() function.
/// Inside a lambda, a subcolumn of a lambda parameter becomes getSubcolumn() of the parameter.
void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns);

}

