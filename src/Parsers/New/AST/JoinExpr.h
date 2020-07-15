#pragma once

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/TableExpr.h>


namespace DB::AST
{

class JoinConstraintClause : public INode
{
    public:
        enum class ConstraintType
        {
            ON,
            USING,
        };

        JoinConstraintClause(ConstraintType type, PtrTo<ColumnExprList> list);

    private:
        ConstraintType type;
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
        enum class ExprType
        {
            TABLE,
            JOIN_OP,
        };
        ExprType expr_type;

        JoinExpr(ExprType type, std::list<Ptr> exprs);
};

}
