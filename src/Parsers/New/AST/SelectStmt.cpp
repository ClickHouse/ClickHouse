#include <Parsers/New/AST/SelectStmt.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/New/AST/JoinExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// WITH Clause

WithClause::WithClause(PtrTo<ColumnExprList> expr_list) : exprs(expr_list)
{
}

// FROM Clause

FromClause::FromClause(PtrTo<JoinExpr> join_expr, bool final_) : expr(join_expr), final(final_)
{
    /// FIXME: remove this.
    (void)final;
}

ASTPtr FromClause::convertToOld() const
{
    auto old_tables = std::make_shared<ASTTablesInSelectQuery>();
    old_tables->children = expr->convertToOld()->children;
    return old_tables;
}

// SAMPLE Clause

SampleClause::SampleClause(PtrTo<RatioExpr> ratio_) : ratio(ratio_)
{
}

SampleClause::SampleClause(PtrTo<RatioExpr> ratio_, PtrTo<RatioExpr> offset_) : ratio(ratio_), offset(offset_)
{
}

// ARRAY JOIN Clause

ArrayJoinClause::ArrayJoinClause(PtrTo<ColumnExprList> expr_list, bool left_) : exprs(expr_list), left(left_)
{
    /// FIXME: remove this.
    (void)left;
}

// PREWHERE Clause

PrewhereClause::PrewhereClause(PtrTo<ColumnExpr> expr_) : expr(expr_)
{
}

// WHERE Clause

WhereClause::WhereClause(PtrTo<ColumnExpr> expr_) : expr(expr_)
{
}

// GROUP BY Clause

GroupByClause::GroupByClause(PtrTo<ColumnExprList> expr_list, bool with_totals_) : exprs(expr_list), with_totals(with_totals_)
{
    /// FIXME: remove this.
    (void)with_totals;
}

// HAVING Clause

HavingClause::HavingClause(PtrTo<ColumnExpr> expr_) : expr(expr_)
{
}

// ORDER BY Clause

OrderByClause::OrderByClause(PtrTo<OrderExprList> expr_list) : exprs(expr_list)
{
}

// LIMIT By Clause

LimitByClause::LimitByClause(PtrTo<LimitExpr> expr, PtrTo<ColumnExprList> expr_list) : limit(expr), by(expr_list)
{
}

// LIMIT Clause

LimitClause::LimitClause(PtrTo<LimitExpr> expr_) : expr(expr_)
{
}

// SETTINGS Clause

SettingsClause::SettingsClause(PtrTo<SettingExprList> expr_list) : exprs(expr_list)
{
}

// SELECT Statement

SelectStmt::SelectStmt(PtrTo<ColumnExprList> expr_list)
{
    children.resize(MAX_INDEX);

    children[COLUMNS] = expr_list;
}

void SelectStmt::setWithClause(PtrTo<WithClause> clause)
{
    children[WITH] = clause;
}

void SelectStmt::setFromClause(PtrTo<FromClause> clause)
{
    children[FROM] = clause;
}

void SelectStmt::setSampleClause(PtrTo<SampleClause> clause)
{
    children[SAMPLE] = clause;
}

void SelectStmt::setArrayJoinClause(PtrTo<ArrayJoinClause> clause)
{
    children[ARRAY_JOIN] = clause;
}

void SelectStmt::setPrewhereClause(PtrTo<PrewhereClause> clause)
{
    children[PREWHERE] = clause;
}

void SelectStmt::setWhereClause(PtrTo<WhereClause> clause)
{
    children[WHERE] = clause;
}

void SelectStmt::setGroupByClause(PtrTo<GroupByClause> clause)
{
    children[GROUP_BY] = clause;
}

void SelectStmt::setHavingClause(PtrTo<HavingClause> clause)
{
    children[HAVING] = clause;
}

void SelectStmt::setOrderByClause(PtrTo<OrderByClause> clause)
{
    children[ORDER_BY] = clause;
}

void SelectStmt::setLimitByClause(PtrTo<LimitByClause> clause)
{
    children[LIMIT_BY] = clause;
}

void SelectStmt::setLimitClause(PtrTo<LimitClause> clause)
{
    children[LIMIT] = clause;
}

void SelectStmt::setSettingsClause(PtrTo<SettingsClause> clause)
{
    children[SETTINGS] = clause;
}

ASTPtr SelectStmt::convertToOld() const
{
    auto old_select = std::make_shared<ASTSelectQuery>();

    old_select->setExpression(ASTSelectQuery::Expression::SELECT, children[COLUMNS]->convertToOld());

    if (children[WITH]) old_select->setExpression(ASTSelectQuery::Expression::WITH, children[WITH]->convertToOld());
    if (children[FROM]) old_select->setExpression(ASTSelectQuery::Expression::TABLES, children[FROM]->convertToOld());
    // TODO: SAMPLE
    // TODO: ARRAY JOIN
    if (children[PREWHERE]) old_select->setExpression(ASTSelectQuery::Expression::PREWHERE, children[PREWHERE]->convertToOld());
    if (children[WHERE]) old_select->setExpression(ASTSelectQuery::Expression::WHERE, children[WHERE]->convertToOld());
    if (children[GROUP_BY]) old_select->setExpression(ASTSelectQuery::Expression::GROUP_BY, children[GROUP_BY]->convertToOld());
    if (children[HAVING]) old_select->setExpression(ASTSelectQuery::Expression::HAVING, children[HAVING]->convertToOld());
    if (children[ORDER_BY]) old_select->setExpression(ASTSelectQuery::Expression::ORDER_BY, children[ORDER_BY]->convertToOld());
    // TODO: LIMIT BY
    // TODO: LIMIT
    if (children[SETTINGS]) old_select->setExpression(ASTSelectQuery::Expression::SETTINGS, children[SETTINGS]->convertToOld());

    return old_select;
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitWithClause(ClickHouseParser::WithClauseContext *ctx)
{
    return std::make_shared<AST::WithClause>(ctx->columnExprList()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitFromClause(ClickHouseParser::FromClauseContext *ctx)
{
    return std::make_shared<AST::FromClause>(ctx->joinExpr()->accept(this), !!ctx->FINAL());
}

antlrcpp::Any ParseTreeVisitor::visitSampleClause(ClickHouseParser::SampleClauseContext *ctx)
{
    if (ctx->OFFSET()) return std::make_shared<AST::SampleClause>(ctx->ratioExpr(0)->accept(this), ctx->ratioExpr(1)->accept(this));
    else return std::make_shared<AST::SampleClause>(ctx->ratioExpr(0)->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitArrayJoinClause(ClickHouseParser::ArrayJoinClauseContext *ctx)
{
    return std::make_shared<AST::ArrayJoinClause>(ctx->columnExprList()->accept(this), !!ctx->LEFT());
}

antlrcpp::Any ParseTreeVisitor::visitPrewhereClause(ClickHouseParser::PrewhereClauseContext *ctx)
{
    return std::make_shared<AST::PrewhereClause>(ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitWhereClause(ClickHouseParser::WhereClauseContext *ctx)
{
    return std::make_shared<AST::WhereClause>(ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitGroupByClause(ClickHouseParser::GroupByClauseContext *ctx)
{
    return std::make_shared<AST::GroupByClause>(ctx->columnExprList()->accept(this), !!ctx->TOTALS());
}

antlrcpp::Any ParseTreeVisitor::visitHavingClause(ClickHouseParser::HavingClauseContext *ctx)
{
    return std::make_shared<AST::HavingClause>(ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitOrderByClause(ClickHouseParser::OrderByClauseContext *ctx)
{
    return std::make_shared<AST::OrderByClause>(ctx->orderExprList()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitLimitByClause(ClickHouseParser::LimitByClauseContext *ctx)
{
    return std::make_shared<AST::LimitByClause>(ctx->limitExpr()->accept(this), ctx->columnExprList()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitLimitClause(ClickHouseParser::LimitClauseContext *ctx)
{
    return std::make_shared<AST::LimitClause>(ctx->limitExpr()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitSettingsClause(ClickHouseParser::SettingsClauseContext *ctx)
{
    return std::make_shared<AST::SettingsClause>(ctx->settingExprList()->accept(this));
}

}
