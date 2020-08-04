#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class TableExpr : public INode
{
    public:
        static PtrTo<TableExpr> createIdentifier(PtrTo<TableIdentifier> identifier);
        static PtrTo<TableExpr> createFunction(PtrTo<Identifier> name, PtrList args);
        static PtrTo<TableExpr> createSubquery(PtrTo<SelectStmt> subquery);
        static PtrTo<TableExpr> createAlias(PtrTo<TableExpr> expr, PtrTo<Identifier> alias);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            IDENTIFIER = 0,
            SUBQUERY = 0,
        };
        enum class ExprType
        {
            IDENTIFIER,
            FUNCTION,
            SUBQUERY,
            ALIAS,
        };

        ExprType expr_type;

        TableExpr(ExprType type, PtrList exprs);
};

}
