#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class TableExpr : public INode
{
    public:
        static PtrTo<TableExpr> createAlias(PtrTo<TableExpr> expr, PtrTo<Identifier> alias);
        static PtrTo<TableExpr> createFunction(PtrTo<Identifier> name, PtrList args);
        static PtrTo<TableExpr> createIdentifier(PtrTo<TableIdentifier> identifier);
        static PtrTo<TableExpr> createSubquery(PtrTo<SelectStmt> subquery);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            IDENTIFIER = 0,
            SUBQUERY = 0,
        };
        enum class ExprType
        {
            ALIAS,
            FUNCTION,
            IDENTIFIER,
            SUBQUERY,
        };

        ExprType expr_type;

        TableExpr(ExprType type, PtrList exprs);
};

}
