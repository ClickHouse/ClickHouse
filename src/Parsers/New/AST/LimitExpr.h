#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class LimitExpr : public INode
{
    public:
        explicit LimitExpr(PtrTo<NumberLiteral> limit, PtrTo<NumberLiteral> offset = nullptr);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            LIMIT = 0,   // NumberLiteral
            OFFSET = 1,  // NumberLiteral (optional)
        };
};

}
