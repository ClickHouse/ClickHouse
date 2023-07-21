#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query);

}
