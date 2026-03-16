#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

ASTFunction * extractTableFunctionFromSelectQuery(ASTPtr & query);
ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query);

}
