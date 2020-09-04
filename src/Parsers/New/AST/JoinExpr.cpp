#include <Parsers/New/AST/JoinExpr.h>

#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB::ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

namespace DB::AST
{

JoinConstraintClause::JoinConstraintClause(ConstraintType type_, PtrTo<ColumnExprList> list) : type(type_)
{
    children.assign(list->begin(), list->end());
    (void)type; // TODO: remove this.
}

// static
PtrTo<JoinExpr> JoinExpr::createTableExpr(PtrTo<TableExpr> expr)
{
    return PtrTo<JoinExpr>(new JoinExpr(JoinExpr::ExprType::TABLE, {expr}));
}

// static
PtrTo<JoinExpr> JoinExpr::createJoinOp(PtrTo<JoinExpr> left_expr, PtrTo<JoinExpr> right_expr, JoinOpType op, JoinOpMode mode, PtrTo<JoinConstraintClause> clause)
{
    return PtrTo<JoinExpr>(new JoinExpr(ExprType::JOIN_OP, op, mode, {left_expr, right_expr, clause}));
}

JoinExpr::JoinExpr(JoinExpr::ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;
}

JoinExpr::JoinExpr(JoinExpr::ExprType type, JoinExpr::JoinOpType op, JoinExpr::JoinOpMode mode, PtrList exprs)
    : expr_type(type), op_type(op), op_mode(mode)
{
    children = exprs;
}

ASTPtr JoinExpr::convertToOld() const
{
    /** The sole convertable chain of Join's may look like:
     *
     *      … FROM table1 JOIN table2 ON SMTH JOIN table3 ON SMTH JOIN …
     *
     *  Since Join is a left-associative operation then the tree will look like:
     *
     *                JoinExpr
     *               /       \
     *           JoinExpr     …
     *          /       \
     *      JoinExpr   table3
     *     /       \
     *  table1    table2
     *
     *  To linearize this tree we have to start from the top-most expression.
     */

    auto list = std::make_shared<ASTExpressionList>();

    if (expr_type == ExprType::TABLE)
    {
        auto element = std::make_shared<ASTTablesInSelectQueryElement>();
        element->children.emplace_back(children[TABLE]->convertToOld());
        element->table_expression = element->children.back();

        list->children.emplace_back(element);
    }
    else if (expr_type == ExprType::JOIN_OP)
    {
        if (children[RIGHT_EXPR]->as<JoinExpr>()->expr_type != ExprType::TABLE)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Cannot convert new tree-like JoinExpr to old AST");

        auto left = children[LEFT_EXPR]->convertToOld(), right = children[RIGHT_EXPR]->convertToOld();  // ASTExpressionList's
        list->children.insert(list->children.end(), left->children.begin(), left->children.end());  // Insert all the previously parsed left subtree
        list->children.emplace_back(right->children[0]);  // Insert only first (single) ASTTablesInSelectQueryElement which should contain only ASTTableExpression

        auto element = std::make_shared<ASTTableJoin>();
        switch (op_mode)
        {
            case JoinOpMode::DEFAULT:
                element->locality = ASTTableJoin::Locality::Unspecified;
                break;
            case JoinOpMode::GLOBAL:
                element->locality = ASTTableJoin::Locality::Global;
                break;
            case JoinOpMode::LOCAL:
                element->locality = ASTTableJoin::Locality::Local;
                break;
        }
        switch (op_type)
        {
            case JoinOpType::CROSS:
                element->kind = ASTTableJoin::Kind::Cross;
                break;
            case JoinOpType::FULL:
                element->kind = ASTTableJoin::Kind::Full;
                break;
            case JoinOpType::FULL_ANY:
                element->kind = ASTTableJoin::Kind::Full;
                element->strictness = ASTTableJoin::Strictness::Any;
                break;
            case JoinOpType::FULL_OUTER:
                element->kind = ASTTableJoin::Kind::Full;
                // TODO: looks like not supported.
                break;
            case JoinOpType::INNER:
                element->kind = ASTTableJoin::Kind::Inner;
                break;
            case JoinOpType::INNER_ANY:
                element->kind = ASTTableJoin::Kind::Inner;
                element->strictness = ASTTableJoin::Strictness::Any;
                break;
            case JoinOpType::LEFT:
                element->kind = ASTTableJoin::Kind::Left;
                break;
            case JoinOpType::LEFT_ANTI:
                element->kind = ASTTableJoin::Kind::Left;
                element->strictness = ASTTableJoin::Strictness::Anti;
                break;
            case JoinOpType::LEFT_ANY:
                element->kind = ASTTableJoin::Kind::Left;
                element->strictness = ASTTableJoin::Strictness::Any;
                break;
            case JoinOpType::LEFT_ASOF:
                element->kind = ASTTableJoin::Kind::Left;
                element->strictness = ASTTableJoin::Strictness::Asof;
                break;
            case JoinOpType::LEFT_OUTER:
                element->kind = ASTTableJoin::Kind::Left;
                // TODO: looks like not supported.
                break;
            case JoinOpType::LEFT_SEMI:
                element->kind = ASTTableJoin::Kind::Left;
                element->strictness = ASTTableJoin::Strictness::Semi;
                break;
            case JoinOpType::RIGHT:
                element->kind = ASTTableJoin::Kind::Right;
                break;
            case JoinOpType::RIGHT_ANTI:
                element->kind = ASTTableJoin::Kind::Right;
                element->strictness = ASTTableJoin::Strictness::Anti;
                break;
            case JoinOpType::RIGHT_ANY:
                element->kind = ASTTableJoin::Kind::Right;
                element->strictness = ASTTableJoin::Strictness::Any;
                break;
            case JoinOpType::RIGHT_ASOF:
                element->kind = ASTTableJoin::Kind::Right;
                element->strictness = ASTTableJoin::Strictness::Asof;
                break;
            case JoinOpType::RIGHT_OUTER:
                element->kind = ASTTableJoin::Kind::Right;
                // TODO: looks like not supported.
                break;
            case JoinOpType::RIGHT_SEMI:
                element->kind = ASTTableJoin::Kind::Right;
                element->strictness = ASTTableJoin::Strictness::Semi;
                break;
        }

        // TODO: convert USING or ON expressions.

        list->children.back()->children.emplace_back(element);
        list->children.back()->as<ASTTablesInSelectQueryElement>()->table_join = element;
    }

    return list;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitJoinConstraintClause(ClickHouseParser::JoinConstraintClauseContext *ctx)
{
    return std::make_shared<JoinConstraintClause>(
        ctx->ON() ? JoinConstraintClause::ConstraintType::ON : JoinConstraintClause::ConstraintType::USING,
        visit(ctx->columnExprList()));
}

antlrcpp::Any ParseTreeVisitor::visitJoinExprCrossOp(ClickHouseParser::JoinExprCrossOpContext *ctx)
{
    auto [op, mode] = std::pair<JoinExpr::JoinOpType, JoinExpr::JoinOpMode>(visit(ctx->joinOpCross()));

    return JoinExpr::createJoinOp(visit(ctx->joinExpr(0)), visit(ctx->joinExpr(1)), op, mode, nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitJoinExprOp(ClickHouseParser::JoinExprOpContext *ctx)
{
    auto mode = JoinExpr::JoinOpMode::DEFAULT;
    auto op = ctx->joinOp() ? visit(ctx->joinOp()).as<JoinExpr::JoinOpType>() : JoinExpr::JoinOpType::LEFT;

    if (ctx->GLOBAL()) mode = JoinExpr::JoinOpMode::GLOBAL;
    else if (ctx->LOCAL()) mode = JoinExpr::JoinOpMode::LOCAL;

    return JoinExpr::createJoinOp(visit(ctx->joinExpr(0)), visit(ctx->joinExpr(1)), op, mode, visit(ctx->joinConstraintClause()));
}

antlrcpp::Any ParseTreeVisitor::visitJoinExprParens(ClickHouseParser::JoinExprParensContext *ctx)
{
    return visit(ctx->joinExpr());
}

antlrcpp::Any ParseTreeVisitor::visitJoinExprTable(ClickHouseParser::JoinExprTableContext *ctx)
{
    return JoinExpr::createTableExpr(visit(ctx->tableExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitJoinOpCross(ClickHouseParser::JoinOpCrossContext *ctx)
{
    std::pair<JoinExpr::JoinOpType, JoinExpr::JoinOpMode> op{
        JoinExpr::JoinOpType::CROSS, JoinExpr::JoinOpMode::DEFAULT};

    if (ctx->GLOBAL()) op.second = JoinExpr::JoinOpMode::GLOBAL;
    else if (ctx->LOCAL()) op.second = JoinExpr::JoinOpMode::LOCAL;

    return op;
}

antlrcpp::Any ParseTreeVisitor::visitJoinOpFull(ClickHouseParser::JoinOpFullContext *ctx)
{
    if (ctx->OUTER()) return JoinExpr::JoinOpType::FULL_OUTER;
    if (ctx->ANY()) return JoinExpr::JoinOpType::FULL_ANY;
    return JoinExpr::JoinOpType::FULL;
}

antlrcpp::Any ParseTreeVisitor::visitJoinOpInner(ClickHouseParser::JoinOpInnerContext *ctx)
{
    if (ctx->ANY()) return JoinExpr::JoinOpType::INNER_ANY;
    return JoinExpr::JoinOpType::INNER;
}

antlrcpp::Any ParseTreeVisitor::visitJoinOpLeftRight(ClickHouseParser::JoinOpLeftRightContext *ctx)
{
    if (ctx->LEFT())
    {
        if (ctx->OUTER()) return JoinExpr::JoinOpType::LEFT_OUTER;
        if (ctx->SEMI()) return JoinExpr::JoinOpType::LEFT_SEMI;
        if (ctx->ANTI()) return JoinExpr::JoinOpType::LEFT_ANTI;
        if (ctx->ANY()) return JoinExpr::JoinOpType::LEFT_ANY;
        if (ctx->ASOF()) return JoinExpr::JoinOpType::LEFT_ASOF;
        return JoinExpr::JoinOpType::LEFT;
    }
    else if (ctx->RIGHT())
    {
        if (ctx->OUTER()) return JoinExpr::JoinOpType::RIGHT_OUTER;
        if (ctx->SEMI()) return JoinExpr::JoinOpType::RIGHT_SEMI;
        if (ctx->ANTI()) return JoinExpr::JoinOpType::RIGHT_ANTI;
        if (ctx->ANY()) return JoinExpr::JoinOpType::RIGHT_ANY;
        if (ctx->ASOF()) return JoinExpr::JoinOpType::RIGHT_ASOF;
        return JoinExpr::JoinOpType::RIGHT;
    }
    __builtin_unreachable();
}

}
