#pragma once

#include <Parsers/New/AST/Literal.h>


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
