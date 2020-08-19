#pragma once

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>


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

        auto getType() const { return expr_type; };

        // FUNCTION
        auto getFunctionName() const { return children[NAME]->as<Identifier>()->getName(); }
        auto argumentsBegin() const { return children[ARGS] ? children[ARGS]->as<ColumnExprList>()->begin() : children.end(); }
        auto argumentsEnd() const { return children[ARGS] ? children[ARGS]->as<ColumnExprList>()->end() : children.end(); }

        // LITERAL
        auto getLiteral() const { return static_pointer_cast<Literal>(children[LITERAL]); }

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

        const ExprType expr_type;
        bool expect_single_column = false;

        ColumnExpr(ExprType type, PtrList exprs);

        String dumpInfo() const override;
};

}
