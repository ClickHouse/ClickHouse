#include <memory>
#include <Parsers/New/AST/SelectStmt.h>

#include <Parsers/New/parseQuery.h>
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/New/AST/INode.h"
#include "Parsers/New/AST/Identifier.h"
#include "Parsers/New/AST/Literal.h"


namespace DB::AST
{

JoinExpr::JoinExpr(PtrTo<TableIdentifier> id) : table(id)
{
}

RatioExpr::RatioExpr(PtrTo<NumberLiteral> num) : num1(num)
{
}

RatioExpr::RatioExpr(PtrTo<NumberLiteral> num1_, PtrTo<NumberLiteral> num2_) : num1(num1_), num2(num2_)
{
}

OrderExpr::OrderExpr(PtrTo<ColumnExpr> expr_, NullsOrder nulls_, PtrTo<StringLiteral> collate_, bool ascending)
    : expr(expr_), nulls(nulls_), collate(collate_), asc(ascending)
{
    /// FIXME: remove this.
    (void)nulls;
    (void)asc;
}

LimitExpr::LimitExpr(PtrTo<NumberLiteral> limit_) : limit(limit_)
{
}

LimitExpr::LimitExpr(PtrTo<NumberLiteral> limit_, PtrTo<NumberLiteral> offset_) : limit(limit_), offset(offset_)
{
}

SettingExpr::SettingExpr(PtrTo<Identifier> name_, PtrTo<Literal> value_) : name(name_), value(value_)
{
}

WithClause::WithClause(PtrTo<ColumnExprList> expr_list) : exprs(expr_list)
{
}

FromClause::FromClause(PtrTo<JoinExpr> join_expr, bool final_) : expr(join_expr), final(final_)
{
    /// FIXME: remove this.
    (void)final;
}

SampleClause::SampleClause(PtrTo<RatioExpr> ratio_) : ratio(ratio_)
{
}

SampleClause::SampleClause(PtrTo<RatioExpr> ratio_, PtrTo<RatioExpr> offset_) : ratio(ratio_), offset(offset_)
{
}

ArrayJoinClause::ArrayJoinClause(PtrTo<ColumnExprList> expr_list, bool left_) : exprs(expr_list), left(left_)
{
    /// FIXME: remove this.
    (void)left;
}

PrewhereClause::PrewhereClause(PtrTo<ColumnExpr> expr_) : expr(expr_)
{
}

WhereClause::WhereClause(PtrTo<ColumnExpr> expr_) : expr(expr_)
{
}

GroupByClause::GroupByClause(PtrTo<ColumnExprList> expr_list, bool with_totals_) : exprs(expr_list), with_totals(with_totals_)
{
    /// FIXME: remove this.
    (void)with_totals;
}

HavingClause::HavingClause(PtrTo<ColumnExpr> expr_) : expr(expr_)
{
}

OrderByClause::OrderByClause(PtrTo<OrderExprList> expr_list) : exprs(expr_list)
{
}

LimitByClause::LimitByClause(PtrTo<LimitExpr> expr, PtrTo<ColumnExprList> expr_list) : limit(expr), by(expr_list)
{
}

LimitClause::LimitClause(PtrTo<LimitExpr> expr_) : expr(expr_)
{
}

SettingsClause::SettingsClause(PtrTo<SettingExprList> expr_list) : exprs(expr_list)
{
}

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

    old_select->setExpression(ASTSelectQuery::Expression::WITH, with->convertToOld());
    old_select->setExpression(ASTSelectQuery::Expression::SELECT, columns->convertToOld());
    old_select->setExpression(ASTSelectQuery::Expression::TABLES, from->convertToOld());
    // TODO: SAMPLE
    // TODO: ARRAY JOIN
    old_select->setExpression(ASTSelectQuery::Expression::PREWHERE, prewhere->convertToOld());
    old_select->setExpression(ASTSelectQuery::Expression::WHERE, where->convertToOld());
    old_select->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by->convertToOld());
    old_select->setExpression(ASTSelectQuery::Expression::HAVING, having->convertToOld());
    old_select->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_by->convertToOld());
    // TODO: LIMIT BY
    // TODO: LIMIT
    old_select->setExpression(ASTSelectQuery::Expression::SETTINGS, settings->convertToOld());

    return old_select;
}

}

namespace DB
{

antlrcpp::Any ParserTreeVisitor::visitJoinExpr(ClickHouseParser::JoinExprContext *ctx)
{
    /// TODO: not complete!
    return std::make_shared<AST::JoinExpr>(ctx->tableIdentifier()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitRatioExpr(ClickHouseParser::RatioExprContext *ctx)
{
    /// TODO: not complete!
    if (ctx->SLASH()) return std::make_shared<AST::RatioExpr>(ctx->NUMBER_LITERAL(0)->accept(this), ctx->NUMBER_LITERAL(1)->accept(this));
    else return std::make_shared<AST::RatioExpr>(ctx->NUMBER_LITERAL(0)->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitOrderExprList(ClickHouseParser::OrderExprListContext *ctx)
{
    auto expr_list = std::make_shared<AST::OrderExprList>();
    for (auto* expr : ctx->orderExpr()) expr_list->append(expr->accept(this));
    return expr_list;
}

antlrcpp::Any ParserTreeVisitor::visitOrderExpr(ClickHouseParser::OrderExprContext *ctx)
{
    AST::OrderExpr::NullsOrder nulls = AST::OrderExpr::NATURAL;
    if (ctx->FIRST()) nulls = AST::OrderExpr::NULLS_FIRST;
    else if (ctx->LAST()) nulls = AST::OrderExpr::NULLS_LAST;

    AST::PtrTo<AST::StringLiteral> collate;
    if (ctx->COLLATE()) collate = std::make_shared<AST::StringLiteral>(ctx->STRING_LITERAL()->accept(this));

    return std::make_shared<AST::OrderExpr>(ctx->columnExpr()->accept(this), nulls, collate, !ctx->DESCENDING());
}

antlrcpp::Any ParserTreeVisitor::visitLimitExpr(ClickHouseParser::LimitExprContext *ctx)
{
    if (ctx->OFFSET()) return std::make_shared<AST::LimitExpr>(ctx->NUMBER_LITERAL(0)->accept(this), ctx->NUMBER_LITERAL(1)->accept(this));
    else return std::make_shared<AST::LimitExpr>(ctx->NUMBER_LITERAL(0)->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitSettingExprList(ClickHouseParser::SettingExprListContext *ctx)
{
    auto expr_list = std::make_shared<AST::SettingExprList>();
    for (auto* expr : ctx->settingExpr()) expr_list->append(expr->accept(this));
    return expr_list;
}

antlrcpp::Any ParserTreeVisitor::visitSettingExpr(ClickHouseParser::SettingExprContext *ctx)
{
    return std::make_shared<AST::SettingExpr>(ctx->identifier()->accept(this), ctx->LITERAL()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitWithClause(ClickHouseParser::WithClauseContext *ctx)
{
    return std::make_shared<AST::WithClause>(ctx->columnExprList()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitFromClause(ClickHouseParser::FromClauseContext *ctx)
{
    return std::make_shared<AST::FromClause>(ctx->joinExpr()->accept(this), !!ctx->FINAL());
}

antlrcpp::Any ParserTreeVisitor::visitSampleClause(ClickHouseParser::SampleClauseContext *ctx)
{
    if (ctx->OFFSET()) return std::make_shared<AST::SampleClause>(ctx->ratioExpr(0)->accept(this), ctx->ratioExpr(1)->accept(this));
    else return std::make_shared<AST::SampleClause>(ctx->ratioExpr(0)->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitArrayJoinClause(ClickHouseParser::ArrayJoinClauseContext *ctx)
{
    return std::make_shared<AST::ArrayJoinClause>(ctx->columnExprList()->accept(this), !!ctx->LEFT());
}

antlrcpp::Any ParserTreeVisitor::visitPrewhereClause(ClickHouseParser::PrewhereClauseContext *ctx)
{
    return std::make_shared<AST::PrewhereClause>(ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitWhereClause(ClickHouseParser::WhereClauseContext *ctx)
{
    return std::make_shared<AST::WhereClause>(ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitGroupByClause(ClickHouseParser::GroupByClauseContext *ctx)
{
    return std::make_shared<AST::GroupByClause>(ctx->columnExprList()->accept(this), !!ctx->TOTALS());
}

antlrcpp::Any ParserTreeVisitor::visitHavingClause(ClickHouseParser::HavingClauseContext *ctx)
{
    return std::make_shared<AST::HavingClause>(ctx->columnExpr()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitOrderByClause(ClickHouseParser::OrderByClauseContext *ctx)
{
    return std::make_shared<AST::OrderByClause>(ctx->orderExprList()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitLimitByClause(ClickHouseParser::LimitByClauseContext *ctx)
{
    return std::make_shared<AST::LimitByClause>(ctx->limitExpr()->accept(this), ctx->columnExprList()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitLimitClause(ClickHouseParser::LimitClauseContext *ctx)
{
    return std::make_shared<AST::LimitClause>(ctx->limitExpr()->accept(this));
}

antlrcpp::Any ParserTreeVisitor::visitSettingsClause(ClickHouseParser::SettingsClauseContext *ctx)
{
    return std::make_shared<AST::SettingsClause>(ctx->settingExprList()->accept(this));
}

}
