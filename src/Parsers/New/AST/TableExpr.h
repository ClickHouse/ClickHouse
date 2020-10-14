#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class TableArgExpr : public INode
{
    public:
        explicit TableArgExpr(PtrTo<Literal> literal);
        explicit TableArgExpr(PtrTo<TableFunctionExpr> function);
        explicit TableArgExpr(PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPR = 0,  // Literal or TableFunctionExpr or TableIdentifier
        };
};

class TableExpr : public INode
{
    public:
        static PtrTo<TableExpr> createAlias(PtrTo<TableExpr> expr, PtrTo<Identifier> alias);
        static PtrTo<TableExpr> createFunction(PtrTo<TableFunctionExpr> function);
        static PtrTo<TableExpr> createIdentifier(PtrTo<TableIdentifier> identifier);
        static PtrTo<TableExpr> createSubquery(PtrTo<SelectUnionQuery> subquery);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // ALIAS
            EXPR = 0,   // TableExpr
            ALIAS = 1,  // Identifier

            // FUNCTION
            FUNCTION = 0,  // TableFunctionExpr

            // IDENTIFIER
            IDENTIFIER = 0,  // TableIdentifier

            // SUBQUERY
            SUBQUERY = 0,  // SelectUnionSubquery
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

        String dumpInfo() const override;
};

class TableFunctionExpr : public INode
{
    public:
        TableFunctionExpr(PtrTo<Identifier> name, PtrTo<TableArgList> args);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            NAME = 0,
            ARGS = 1,
        };
};

}
