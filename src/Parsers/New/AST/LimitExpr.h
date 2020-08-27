#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class LimitExpr : public INode
{
    public:
        explicit LimitExpr(PtrTo<NumberLiteral> limit, PtrTo<NumberLiteral> offset = nullptr);

    private:
        enum ChildIndex : UInt8
        {
            LIMIT = 0,
            OFFSET = 1,
        };
};

}
