#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class OrderExpr : public INode
{
    public:
        enum NullsOrder {
            NATURAL,
            NULLS_FIRST,
            NULLS_LAST,
        };

        OrderExpr(PtrTo<ColumnExpr> expr, NullsOrder nulls_, PtrTo<StringLiteral> collate, bool ascending = true);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPR = 0,  // ColumnExpr
            COLLATE,   // StringLiteral (optional)
        };

        NullsOrder nulls;
        bool asc;
};

}
