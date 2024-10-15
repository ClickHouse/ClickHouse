#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query);

}
