#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/// It converts some arithmetic .
class ArithmeticOperationsInAgrFuncVisitor
{
public:
    ArithmeticOperationsInAgrFuncVisitor() = default;

    void visit(ASTPtr &ast);

};

}
