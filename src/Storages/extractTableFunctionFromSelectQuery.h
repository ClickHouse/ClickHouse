#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

ASTFunction * extractTableFunctionFromSelectQuery(ASTPtr & query);

}
