#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class ColumnExpr : public INode
{
    public:
        static PtrTo<ColumnExpr> createAlias(PtrTo<ColumnExpr> expr, PtrTo<Identifier> alias);
        static PtrTo<ColumnExpr> createAsterisk(PtrTo<TableIdentifier> identifier, bool single_column);
        static PtrTo<ColumnExpr> createFunction(PtrTo<Identifier> name, PtrTo<ColumnParamList> params, PtrTo<ColumnExprList> args);
        static PtrTo<ColumnExpr> createIdentifier(PtrTo<ColumnIdentifier> identifier);
        static PtrTo<ColumnExpr> createLambda(PtrTo<List<Identifier, ','>> params, PtrTo<ColumnExpr> expr);
        static PtrTo<ColumnExpr> createLiteral(PtrTo<Literal> literal);
        static PtrTo<ColumnExpr> createSubquery(PtrTo<SelectUnionQuery> query, bool scalar);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // ALIAS
            EXPR = 0,
            ALIAS = 1,

            // IDENTIFIER
            IDENTIFIER = 0,

            // FUNCTION
            NAME = 0,
            PARAMS = 1,  // may be |nullptr|
            ARGS = 2,    // may be |nullptr|

            // LAMBDA
            LAMBDA_ARGS = 0,
            LAMBDA_EXPR = 1,

            // LITERAL
            LITERAL = 0,

            // SUBQUERY
            SUBQUERY = 0,
        };
        enum class ExprType
        {
            ALIAS,
            ASTERISK,
            FUNCTION,
            IDENTIFIER,
            LAMBDA,
            LITERAL,
            SUBQUERY,
        };

        const ExprType expr_type;
        bool expect_single_column = false;

        ColumnExpr(ExprType type, PtrList exprs);

        String dumpInfo() const override;
};

}
