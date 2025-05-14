#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{
struct ASTTableExpression;

ASTTableExpression * extractTableExpressionASTPtrFromSelectQuery(ASTPtr & query);
ASTPtr extractTableFunctionASTPtrFromSelectQuery(ASTPtr & query);
ASTFunction * extractTableFunctionFromSelectQuery(ASTPtr & query);
ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query);

}
