#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query);
ASTFunction * extractTableFunctionFromSelectQuery(ASTPtr & query);

}
