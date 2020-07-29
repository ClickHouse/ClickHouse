#pragma once

#include <Parsers/New/AST/INode.h>

#include <list>


namespace DB::AST
{

class ColumnExpr;
using ColumnExprList = List<ColumnExpr, ','>;
class ColumnFunctionExpr;
class ColumnIdentifier;
class Identifier;
class Literal;
class NumberLiteral;

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
            ASTERISK,
            SLASH,
            PERCENT,
            PLUS,
            DASH,
            EQ,
            NOT_EQ,
            LE,
            GE,
            LT,
            GT,
            CONCAT,
            AND,
            OR,
            LIKE,
            NOT_LIKE,
            IN,
            NOT_IN,
            GLOBAL_IN,
            GLOBAL_NOT_IN,
        };

        enum class IntervalType
        {
            SECOND,
            MINUTE,
            HOUR,
            DAY,
            WEEK,
            MONTH,
            QUARTER,
            YEAR,
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
        static PtrTo<ColumnExpr> createInterval(IntervalType type, PtrTo<ColumnExpr> expr);
        static PtrTo<ColumnExpr> createFunction(PtrTo<ColumnFunctionExpr> expr);
        static PtrTo<ColumnExpr> createAlias(PtrTo<ColumnExpr> expr, PtrTo<Identifier> alias);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            LITERAL = 0,
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
            BETWEEN,
            CASE,
            INTERVAL,
            FUNCTION,
            ALIAS,
        };

        ExprType expr_type;

        ColumnExpr(ExprType type, std::vector<Ptr> exprs);
};

}
