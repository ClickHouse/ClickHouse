#include <Parsers/New/AST/JoinExpr.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/New/AST/RatioExpr.h>
#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

namespace DB::AST
{

JoinConstraintClause::JoinConstraintClause(ConstraintType type_, PtrTo<ColumnExprList> list) : SimpleClause{list}, type(type_)
{
}

SampleClause::SampleClause(PtrTo<RatioExpr> ratio, PtrTo<RatioExpr> offset) : INode{ratio, offset}
{
}

ASTPtr SampleClause::convertToOld() const
{
    auto list = std::make_shared<ASTExpressionList>();

    list->children.push_back(get(RATIO)->convertToOld());
    if (has(OFFSET)) list->children.push_back(get(OFFSET)->convertToOld());

    return list;
}

// static
PtrTo<JoinExpr> JoinExpr::createTableExpr(PtrTo<TableExpr> expr, PtrTo<SampleClause> clause, bool final)
{
    return PtrTo<JoinExpr>(new JoinExpr(JoinExpr::ExprType::TABLE, final, {expr, clause}));
}

// static
PtrTo<JoinExpr> JoinExpr::createJoinOp(
    PtrTo<JoinExpr> left_expr, PtrTo<JoinExpr> right_expr, JoinOpType op, JoinOpMode mode, PtrTo<JoinConstraintClause> clause)
{
    return PtrTo<JoinExpr>(new JoinExpr(ExprType::JOIN_OP, op, mode, {left_expr, right_expr, clause}));
}

JoinExpr::JoinExpr(JoinExpr::ExprType type, bool final_, PtrList exprs) : INode(exprs), expr_type(type), final(final_)
{
}

JoinExpr::JoinExpr(JoinExpr::ExprType type, JoinExpr::JoinOpType op, JoinExpr::JoinOpMode mode, PtrList exprs)
    : INode(exprs), expr_type(type), op_type(op), op_mode(mode)
{
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
        element->children.emplace_back(get(TABLE)->convertToOld());
        element->table_expression = element->children.back();
        element->table_expression->as<ASTTableExpression>()->final = final;
        if (has(SAMPLE))
        {
            auto old_list = get(SAMPLE)->convertToOld();

            element->table_expression->as<ASTTableExpression>()->sample_size = old_list->children[0];
            element->table_expression->children.push_back(element->table_expression->as<ASTTableExpression>()->sample_size);

            if (old_list->children.size() > 1)
            {
                element->table_expression->as<ASTTableExpression>()->sample_offset = old_list->children[1];
                element->table_expression->children.push_back(element->table_expression->as<ASTTableExpression>()->sample_offset);
            }
        }

        list->children.emplace_back(element);
    }
    else if (expr_type == ExprType::JOIN_OP)
    {
        if (get<JoinExpr>(RIGHT_EXPR)->expr_type != ExprType::TABLE)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Cannot convert new tree-like JoinExpr to old AST");

        auto left = get(LEFT_EXPR)->convertToOld(), right = get(RIGHT_EXPR)->convertToOld();  // ASTExpressionList's
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
            case JoinOpType::FULL_ALL:
                element->kind = ASTTableJoin::Kind::Full;
                element->strictness = ASTTableJoin::Strictness::All;
                break;
            case JoinOpType::FULL_ANY:
                element->kind = ASTTableJoin::Kind::Full;
                element->strictness = ASTTableJoin::Strictness::Any;
                break;
            case JoinOpType::INNER:
                element->kind = ASTTableJoin::Kind::Inner;
                break;
            case JoinOpType::INNER_ALL:
                element->kind = ASTTableJoin::Kind::Inner;
                element->strictness = ASTTableJoin::Strictness::All;
                break;
            case JoinOpType::INNER_ANY:
                element->kind = ASTTableJoin::Kind::Inner;
                element->strictness = ASTTableJoin::Strictness::Any;
                break;
            case JoinOpType::INNER_ASOF:
                element->kind = ASTTableJoin::Kind::Inner;
                element->strictness = ASTTableJoin::Strictness::Asof;
                break;
            case JoinOpType::LEFT:
                element->kind = ASTTableJoin::Kind::Left;
                break;
            case JoinOpType::LEFT_ALL:
                element->kind = ASTTableJoin::Kind::Left;
                element->strictness = ASTTableJoin::Strictness::All;
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
            case JoinOpType::RIGHT_ALL:
                element->kind = ASTTableJoin::Kind::Right;
                element->strictness = ASTTableJoin::Strictness::All;
                break;
            case JoinOpType::RIGHT_ANY:
                element->kind = ASTTableJoin::Kind::Right;
                element->strictness = ASTTableJoin::Strictness::Any;
                break;
            case JoinOpType::RIGHT_ASOF:
                element->kind = ASTTableJoin::Kind::Right;
                element->strictness = ASTTableJoin::Strictness::Asof;
                break;
            case JoinOpType::RIGHT_SEMI:
                element->kind = ASTTableJoin::Kind::Right;
                element->strictness = ASTTableJoin::Strictness::Semi;
                break;
        }

        if (has(CONSTRAINT))
        {
            const auto * constraint = get<JoinConstraintClause>(CONSTRAINT);
            switch(constraint->getType())
            {
                case JoinConstraintClause::ConstraintType::ON:
                    element->on_expression = constraint->convertToOld();
                    if (element->on_expression->children.size() > 1)
                        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Cannot convert JoinExpr with more than one ON expression");
                    element->on_expression = element->on_expression->children[0];
                    element->children.push_back(element->on_expression);
                    break;
                case JoinConstraintClause::ConstraintType::USING:
                    element->using_expression_list = constraint->convertToOld();
                    element->children.push_back(element->using_expression_list);
                    break;
            }
        }

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
    auto op = ctx->joinOp() ? visit(ctx->joinOp()).as<JoinExpr::JoinOpType>() : JoinExpr::JoinOpType::INNER;

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
    auto sample = ctx->sampleClause() ? visit(ctx->sampleClause()).as<PtrTo<SampleClause>>() : nullptr;
    return JoinExpr::createTableExpr(visit(ctx->tableExpr()), sample, !!ctx->FINAL());
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
    if (ctx->ALL()) return JoinExpr::JoinOpType::FULL_ALL;
    if (ctx->ANY()) return JoinExpr::JoinOpType::FULL_ANY;
    return JoinExpr::JoinOpType::FULL;
}

antlrcpp::Any ParseTreeVisitor::visitJoinOpInner(ClickHouseParser::JoinOpInnerContext *ctx)
{
    if (ctx->ALL()) return JoinExpr::JoinOpType::INNER_ALL;
    if (ctx->ANY()) return JoinExpr::JoinOpType::INNER_ANY;
    if (ctx->ASOF()) return JoinExpr::JoinOpType::INNER_ASOF;
    return JoinExpr::JoinOpType::INNER;
}

antlrcpp::Any ParseTreeVisitor::visitJoinOpLeftRight(ClickHouseParser::JoinOpLeftRightContext *ctx)
{
    if (ctx->LEFT())
    {
        if (ctx->SEMI()) return JoinExpr::JoinOpType::LEFT_SEMI;
        if (ctx->ALL()) return JoinExpr::JoinOpType::LEFT_ALL;
        if (ctx->ANTI()) return JoinExpr::JoinOpType::LEFT_ANTI;
        if (ctx->ANY()) return JoinExpr::JoinOpType::LEFT_ANY;
        if (ctx->ASOF()) return JoinExpr::JoinOpType::LEFT_ASOF;
        return JoinExpr::JoinOpType::LEFT;
    }
    else if (ctx->RIGHT())
    {
        if (ctx->SEMI()) return JoinExpr::JoinOpType::RIGHT_SEMI;
        if (ctx->ALL()) return JoinExpr::JoinOpType::RIGHT_ALL;
        if (ctx->ANTI()) return JoinExpr::JoinOpType::RIGHT_ANTI;
        if (ctx->ANY()) return JoinExpr::JoinOpType::RIGHT_ANY;
        if (ctx->ASOF()) return JoinExpr::JoinOpType::RIGHT_ASOF;
        return JoinExpr::JoinOpType::RIGHT;
    }
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitSampleClause(ClickHouseParser::SampleClauseContext *ctx)
{
    auto offset = ctx->ratioExpr().size() == 2 ? visit(ctx->ratioExpr(1)).as<PtrTo<RatioExpr>>() : nullptr;
    return std::make_shared<SampleClause>(visit(ctx->ratioExpr(0)), offset);
}

}
