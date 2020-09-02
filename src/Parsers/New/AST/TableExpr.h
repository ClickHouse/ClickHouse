#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class TableArgExpr : public INode
{
    public:
        explicit TableArgExpr(PtrTo<Literal> literal);
        explicit TableArgExpr(PtrTo<TableExpr> expr);

        ASTPtr convertToOld() const override { return children[0]->convertToOld(); }
};

class TableExpr : public INode
{
    public:
        static PtrTo<TableExpr> createAlias(PtrTo<TableExpr> expr, PtrTo<Identifier> alias);
        static PtrTo<TableExpr> createFunction(PtrTo<Identifier> name, PtrTo<TableArgList> args);
        static PtrTo<TableExpr> createIdentifier(PtrTo<TableIdentifier> identifier);
        static PtrTo<TableExpr> createSubquery(PtrTo<SelectUnionQuery> subquery);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // ALIAS
            EXPR = 0,
            ALIAS = 1,

            // FUNCTION
            NAME = 0,
            ARGS = 1,

            // IDENTIFIER
            IDENTIFIER = 0,

            // SUBQUERY
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
