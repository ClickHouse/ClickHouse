#pragma once

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Replace subcolumns to getSubcolumn() function.
/// Inside a lambda, a subcolumn of a lambda parameter becomes getSubcolumn() of the parameter; with @name_lambda_parameter_fields
/// it is named like the subcolumn, or left as written if the expression uses that name otherwise.
void replaceSubcolumnsToGetSubcolumnFunctionInQuery(ASTPtr & ast, const NamesAndTypesList & columns, bool name_lambda_parameter_fields = false);

}

