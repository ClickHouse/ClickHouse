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
        static PtrTo<ColumnExpr> createLambda(PtrTo<List<Identifier>> params, PtrTo<ColumnExpr> expr);
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
        auto getFunctionName() const { return get<Identifier>(NAME)->getName(); }
        auto argumentsBegin() const { return has(ARGS) ? get<ColumnExprList>(ARGS)->begin() : end(); }
        auto argumentsEnd() const { return has(ARGS) ? get<ColumnExprList>(ARGS)->end() : end(); }

        // LITERAL
        auto getLiteral() const { return std::static_pointer_cast<Literal>(get(LITERAL)); }

        ASTPtr convertToOld() const override;
        String toString() const override;

    private:
        enum ChildIndex : UInt8
        {
            // ALIAS
            EXPR = 0,   // ColumnExpr
            ALIAS = 1,  // Identifier

            // ASTERISK
            TABLE = 0,  // TableIdentifier (optional)

            // IDENTIFIER
            IDENTIFIER = 0,  // ColumnIdentifier

            // FUNCTION
            NAME = 0,    // Identifier
            PARAMS = 1,  // ColumnParamList (optional)
            ARGS = 2,    // ColumnExprList (optional)

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
