#include <Parsers/New/AST/SelectStmt.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
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
    columns = expr_list;
}

void SelectStmt::setWithClause(PtrTo<WithClause> clause)
{
    with = clause;
}

void SelectStmt::setFromClause(PtrTo<FromClause> clause)
{
    from = clause;
}

void SelectStmt::setSampleClause(PtrTo<SampleClause> clause)
{
    sample = clause;
}

void SelectStmt::setArrayJoinClause(PtrTo<ArrayJoinClause> clause)
{
    array_join = clause;
}

void SelectStmt::setPrewhereClause(PtrTo<PrewhereClause> clause)
{
    prewhere = clause;
}

void SelectStmt::setWhereClause(PtrTo<WhereClause> clause)
{
    where = clause;
}

void SelectStmt::setGroupByClause(PtrTo<GroupByClause> clause)
{
    group_by = clause;
}

void SelectStmt::setHavingClause(PtrTo<HavingClause> clause)
{
    having = clause;
}

void SelectStmt::setOrderByClause(PtrTo<OrderByClause> clause)
{
    order_by = clause;
}

void SelectStmt::setLimitByClause(PtrTo<LimitByClause> clause)
{
    limit_by = clause;
}

void SelectStmt::setLimitClause(PtrTo<LimitClause> clause)
{
    limit = clause;
}

void SelectStmt::setSettingsClause(PtrTo<SettingsClause> clause)
{
    settings = clause;
}

ASTPtr SelectStmt::convertToOld() const
{
    auto old_select = std::make_shared<ASTSelectQuery>();

    old_select->setExpression(ASTSelectQuery::Expression::SELECT, columns->convertToOld());

    if (with) old_select->setExpression(ASTSelectQuery::Expression::WITH, with->convertToOld());
    if (from) old_select->setExpression(ASTSelectQuery::Expression::TABLES, from->convertToOld());
    // TODO: SAMPLE
    // TODO: ARRAY JOIN
    if (prewhere) old_select->setExpression(ASTSelectQuery::Expression::PREWHERE, prewhere->convertToOld());
    if (where) old_select->setExpression(ASTSelectQuery::Expression::WHERE, where->convertToOld());
    if (group_by) old_select->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by->convertToOld());
    if (having) old_select->setExpression(ASTSelectQuery::Expression::HAVING, having->convertToOld());
    if (order_by) old_select->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_by->convertToOld());
    // TODO: LIMIT BY
    // TODO: LIMIT
    if (settings) old_select->setExpression(ASTSelectQuery::Expression::SETTINGS, settings->convertToOld());

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
