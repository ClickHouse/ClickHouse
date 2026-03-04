#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
struct ASTTableExpression;

ASTTableExpression * extractTableExpressionASTPtrFromSelectQuery(ASTPtr & query);
ASTPtr extractTableFunctionASTPtrFromSelectQuery(ASTPtr & query);
ASTPtr extractTableASTPtrFromSelectQuery(ASTPtr & query);
ASTFunction * extractTableFunctionFromSelectQuery(ASTPtr & query);

}
