#pragma once

#include <Parsers/New/AST/Literal.h>


namespace DB::AST
{

class RatioExpr : public INode
{
    public:
        explicit RatioExpr(PtrTo<NumberLiteral> num);
        RatioExpr(PtrTo<NumberLiteral> num1, PtrTo<NumberLiteral> num2);

    private:
        PtrTo<NumberLiteral> num1, num2;
};

}
