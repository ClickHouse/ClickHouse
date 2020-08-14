#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class ValueExpr : public INode
{
    public:
        static PtrTo<ValueExpr> createArray(PtrTo<ValueExprList> array);
        static PtrTo<ValueExpr> createLiteral(PtrTo<Literal> literal);
        static PtrTo<ValueExpr> createTuple(PtrTo<ValueExprList> tuple);

    private:
        enum class ExprType
        {
            ARRAY,
            LITERAL,
            TUPLE,
        };

        ExprType expr_type;

        ValueExpr(ExprType type, PtrList exprs);
};

}
