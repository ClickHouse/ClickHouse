#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class ColumnExpr : public INode
{
    public:
        static PtrTo<ColumnExpr> createAlias(PtrTo<ColumnExpr> expr, PtrTo<Identifier> alias);
        static PtrTo<ColumnExpr> createAsterisk();
        static PtrTo<ColumnExpr> createFunction(PtrTo<Identifier> name, PtrTo<ColumnParamList> params, PtrTo<ColumnExprList> args);
        static PtrTo<ColumnExpr> createIdentifier(PtrTo<ColumnIdentifier> identifier);
        static PtrTo<ColumnExpr> createLambda(PtrTo<List<Identifier, ','>> params, PtrTo<ColumnExpr> expr);
        static PtrTo<ColumnExpr> createLiteral(PtrTo<Literal> literal);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // ALIAS
            ALIAS = 1,

            // IDENTIFIER
            IDENTIFIER = 0,

            // FUNCTION
            NAME = 0,
            PARAMS = 1,  // may be |nullptr|
            ARGS = 2,    // may be |nullptr|

            // LITERAL
            LITERAL = 0,
        };
        enum class ExprType
        {
            ALIAS,
            ASTERISK,
            FUNCTION,
            IDENTIFIER,
            LAMBDA,
            LITERAL,
        };

        const ExprType expr_type;

        ColumnExpr(ExprType type, PtrList exprs);

        String dumpInfo() const override;
};

}
