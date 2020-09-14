#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class DataExpr : public INode
{
    public:
        static PtrTo<DataExpr> createData(PtrTo<List<Literal>> list);
        static PtrTo<DataExpr> createJSON(PtrTo<JsonExprList> list);
        static PtrTo<DataExpr> createValues(PtrTo<ColumnExprList> list);

    private:
        enum class ExprType
        {
            DATA,
            JSON,
            VALUES,
        };

        ExprType expr_type;

        DataExpr(ExprType type, PtrList exprs);
};

}
