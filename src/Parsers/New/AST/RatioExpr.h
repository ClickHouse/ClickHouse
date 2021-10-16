#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class RatioExpr : public INode
{
    public:
        RatioExpr(PtrTo<NumberLiteral> num1, PtrTo<NumberLiteral> num2);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NUMERATOR = 0,    // NumberLiteral
            DENOMINATOR = 1,  // NumberLiteral (optional)
        };
};

}
