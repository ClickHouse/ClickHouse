#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class LimitExpr : public INode
{
    public:
        explicit LimitExpr(PtrTo<NumberLiteral> limit_);
        LimitExpr(PtrTo<NumberLiteral> limit_, PtrTo<NumberLiteral> offset_);

    private:
        PtrTo<NumberLiteral> limit, offset;
};

}
