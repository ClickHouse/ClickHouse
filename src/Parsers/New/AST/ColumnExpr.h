#pragma once

#include <Parsers/New/AST/INode.h>

namespace DB::AST
{

class ColumnExpr : public INode
{
    public:
        static PtrTo<ColumnExpr> createLiteral();
        static PtrTo<ColumnExpr> createAsterisk();
        static PtrTo<ColumnExpr> createIdentifier();
        static PtrTo<ColumnExpr> createTuple();
        static PtrTo<ColumnExpr> createSubquery();
        static PtrTo<ColumnExpr> createArray();

        static PtrTo<ColumnExpr> createArrayAccess();
        static PtrTo<ColumnExpr> createTupleAccess();
        static PtrTo<ColumnExpr> createUnaryOp();
        static PtrTo<ColumnExpr> createIsNull();
        static PtrTo<ColumnExpr> createBinaryOp();
        static PtrTo<ColumnExpr> createTernaryOp();
        static PtrTo<ColumnExpr> createBetween();
        static PtrTo<ColumnExpr> createCase();
        static PtrTo<ColumnExpr> createInterval();
        static PtrTo<ColumnExpr> createFunctionCall();
        static PtrTo<ColumnExpr> createAlias();
};

using ColumnExprList = List<ColumnExpr, ','>;

}
