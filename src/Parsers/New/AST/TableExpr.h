#pragma once

#include <Parsers/New/AST/Identifier.h>


namespace DB::AST
{

class TableExpr : public INode
{
    public:
        static PtrTo<TableExpr> createIdentifier(PtrTo<TableIdentifier> identifier);
        static PtrTo<TableExpr> createFunction();
        static PtrTo<TableExpr> createSubquery();
        static PtrTo<TableExpr> createAlias();

    private:
        enum class ExprType
        {
            IDENTIFIER,
            FUNCTION,
            SUBQUERY,
            ALIAS,
        };
        ExprType expr_type;

        TableExpr(ExprType type, std::list<Ptr> exprs);
};

}
