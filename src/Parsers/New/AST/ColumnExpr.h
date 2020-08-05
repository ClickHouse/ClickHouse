#pragma once

#include <Parsers/New/AST/INode.h>

#include <list>


namespace DB::AST
{

class ColumnArgExpr : public INode
{
    public:
        explicit ColumnArgExpr(PtrTo<ColumnExpr> expr);
        explicit ColumnArgExpr(PtrTo<ColumnLambdaExpr> expr);

    private:
        enum class ArgType
        {
            EXPR = 0,
            LAMBDA = 1,
        };

        const ArgType type;
};

class ColumnLambdaExpr : public INode
{
    public:
        ColumnLambdaExpr(PtrTo<List<Identifier, ','>> params, PtrTo<ColumnExpr> expr);

    private:
        enum ChildIndex
        {
            EXPR = 0,
        };
};

class ColumnExpr : public INode
{
    public:
        enum class UnaryOpType
        {
            DASH,
            NOT,
            IS_NULL,
            IS_NOT_NULL,
        };

        enum class BinaryOpType
        {
            CONCAT,
            MULTIPLY,
            DIVIDE,
            MODULO,
            PLUS,
            MINUS,
            EQ,
            NOT_EQ,
            LE,
            GE,
            LT,
            GT,
            AND,
            OR,
            LIKE,
            NOT_LIKE,
            IN,
            NOT_IN,
            GLOBAL_IN,
            GLOBAL_NOT_IN,
        };

        enum class TernaryOpType
        {
            IF,
            BETWEEN,
            NOT_BETWEEN,
        };

        static PtrTo<ColumnExpr> createLiteral(PtrTo<Literal> literal);
        static PtrTo<ColumnExpr> createAsterisk();
        static PtrTo<ColumnExpr> createIdentifier(PtrTo<ColumnIdentifier> identifier);
        static PtrTo<ColumnExpr> createTuple(PtrTo<ColumnExprList> list);
        static PtrTo<ColumnExpr> createArray(PtrTo<ColumnExprList> list);

        static PtrTo<ColumnExpr> createArrayAccess(PtrTo<ColumnExpr> expr1, PtrTo<ColumnExpr> expr2);
        static PtrTo<ColumnExpr> createTupleAccess(PtrTo<ColumnExpr> expr, PtrTo<NumberLiteral> literal);
        static PtrTo<ColumnExpr> createUnaryOp(UnaryOpType op, PtrTo<ColumnExpr> expr);
        static PtrTo<ColumnExpr> createBinaryOp(BinaryOpType op, PtrTo<ColumnExpr> expr1, PtrTo<ColumnExpr> expr2);
        static PtrTo<ColumnExpr> createTernaryOp(PtrTo<ColumnExpr> expr1, PtrTo<ColumnExpr> expr2, PtrTo<ColumnExpr> expr3);
        static PtrTo<ColumnExpr> createBetween(bool not_op, PtrTo<ColumnExpr> expr1, PtrTo<ColumnExpr> expr2, PtrTo<ColumnExpr> expr3);
        static PtrTo<ColumnExpr> createCase(PtrTo<ColumnExpr> expr, std::list<std::pair<PtrTo<ColumnExpr> /* when */, PtrTo<ColumnExpr> /* then */>> cases, PtrTo<ColumnExpr> else_expr);
        static PtrTo<ColumnExpr> createFunction(PtrTo<Identifier> name, PtrTo<ColumnParamList> params, PtrTo<ColumnArgList> args);
        static PtrTo<ColumnExpr> createAlias(PtrTo<ColumnExpr> expr, PtrTo<Identifier> alias);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            // LITERAL
            LITERAL = 0,

            // IDENTIFIER
            IDENTIFIER = 0,

            // ARRAY_ACCESS and TUPLE_ACCESS
            ARRAY = 0,
            TUPLE = 0,
            INDEX = 1,

            // ALIAS
            ALIAS = 1,
        };
        enum class ExprType
        {
            LITERAL,
            ASTERISK,
            IDENTIFIER,
            TUPLE,
            ARRAY,
            ARRAY_ACCESS,
            TUPLE_ACCESS,
            UNARY_OP,
            BINARY_OP,
            TERNARY_OP,
            CASE,         // not merge into function because can have |nullptr| children
            FUNCTION,
            ALIAS,
        };

        const ExprType expr_type;
        union
        {
            UnaryOpType unary_op_type;
            BinaryOpType binary_op_type;
            TernaryOpType ternary_op_type;
        };

        ColumnExpr(ExprType type, PtrList exprs);
        ColumnExpr(UnaryOpType type, Ptr expr);
        ColumnExpr(BinaryOpType type, Ptr expr1, Ptr expr2);
        ColumnExpr(TernaryOpType type, Ptr expr1, Ptr expr2, Ptr expr3);
};

}
