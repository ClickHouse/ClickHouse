#pragma once

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Literal.h>


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

        OrderExpr(PtrTo<ColumnExpr> expr_, NullsOrder nulls_, PtrTo<StringLiteral> collate_, bool ascending = true);

    private:
        PtrTo<ColumnExpr> expr;
        NullsOrder nulls;
        PtrTo<StringLiteral> collate;
        bool asc;
};

using OrderExprList = List<OrderExpr, ','>;

}
