#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class RatioExpr : public INode
{
    public:
        explicit RatioExpr(PtrTo<NumberLiteral> num1, PtrTo<NumberLiteral> num2 = nullptr);
};

}
