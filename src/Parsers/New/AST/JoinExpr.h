#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class JoinConstraintClause : public SimpleClause<ColumnExprList>
{
    public:
        enum class ConstraintType
        {
            ON,
            USING,
        };

        JoinConstraintClause(ConstraintType type, PtrTo<ColumnExprList> list);

        auto getType() const { return type; }

    private:
        const ConstraintType type;
};

class JoinExpr : public INode
{
    public:
        enum class JoinOpType
        {
            INNER,
            INNER_ANY,
            LEFT,
            LEFT_OUTER,
            LEFT_SEMI,
            LEFT_ANTI,
            LEFT_ANY,
            LEFT_ASOF,
            RIGHT,
            RIGHT_OUTER,
            RIGHT_SEMI,
            RIGHT_ANTI,
            RIGHT_ANY,
            RIGHT_ASOF,
            FULL,
            FULL_OUTER,
            FULL_ANY,
            CROSS,
        };
        enum class JoinOpMode
        {
            DEFAULT,  // actual mode depends on setting's 'distributed_product_mode' value
            GLOBAL,
            LOCAL,
        };

        static PtrTo<JoinExpr> createTableExpr(PtrTo<TableExpr> expr);
        static PtrTo<JoinExpr> createJoinOp(PtrTo<JoinExpr> left_expr, PtrTo<JoinExpr> right_expr, JoinOpType op, JoinOpMode mode, PtrTo<JoinConstraintClause> clause);

        ASTPtr convertToOld() const override;  // returns topologically sorted elements as ASTExpressionList

    private:
        enum ChildIndex : UInt8
        {
            TABLE = 0,
            LEFT_EXPR = 0,
            RIGHT_EXPR = 1,
            CONSTRAINT = 2,
        };
        enum class ExprType
        {
            TABLE,
            JOIN_OP,
        };

        ExprType expr_type;
        JoinOpType op_type = JoinOpType::INNER;
        JoinOpMode op_mode = JoinOpMode::DEFAULT;

        JoinExpr(ExprType type, PtrList exprs);
        JoinExpr(ExprType type, JoinOpType op, JoinOpMode mode, PtrList exprs);
};

}
