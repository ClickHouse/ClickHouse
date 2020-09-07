#include <Parsers/New/AST/SelectUnionQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/New/AST/JoinExpr.h>
#include <Parsers/New/AST/LimitExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>

namespace DB::AST
{

// WITH Clause

WithClause::WithClause(PtrTo<ColumnExprList> expr_list) : exprs(expr_list)
{
}

// FROM Clause

FromClause::FromClause(PtrTo<JoinExpr> expr, bool final_) : final(final_)
{
    children.push_back(expr);
    /// FIXME: remove this.
    (void)final;
}

ASTPtr FromClause::convertToOld() const
{
    auto old_tables = std::make_shared<ASTTablesInSelectQuery>();
    old_tables->children = children[EXPR]->convertToOld()->children;
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

LimitClause::LimitClause(PtrTo<LimitExpr> expr)
{
    children.push_back(expr);
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

SelectUnionQuery::SelectUnionQuery(std::list<PtrTo<SelectStmt>> stmts)
{
    children.insert(children.end(), stmts.begin(), stmts.end());
}

void SelectUnionQuery::appendSelect(PtrTo<SelectStmt> stmt)
{
    children.push_back(stmt);
}

ASTPtr SelectUnionQuery::convertToOld() const
{
    auto old_select_union = std::make_shared<ASTSelectWithUnionQuery>();
    old_select_union->list_of_selects = std::make_shared<ASTExpressionList>();
    old_select_union->children.push_back(old_select_union->list_of_selects);

    for (const auto & select : children) old_select_union->list_of_selects->children.push_back(select->convertToOld());

    return old_select_union;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitWithClause(ClickHouseParser::WithClauseContext *ctx)
{
    return std::make_shared<WithClause>(visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>());
}

antlrcpp::Any ParseTreeVisitor::visitFromClause(ClickHouseParser::FromClauseContext *ctx)
{
    return std::make_shared<FromClause>(visit(ctx->joinExpr()), !!ctx->FINAL());
}

antlrcpp::Any ParseTreeVisitor::visitSampleClause(ClickHouseParser::SampleClauseContext *ctx)
{
    if (ctx->OFFSET()) return std::make_shared<SampleClause>(visit(ctx->ratioExpr(0)), visit(ctx->ratioExpr(1)));
    else return std::make_shared<SampleClause>(visit(ctx->ratioExpr(0)).as<PtrTo<RatioExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitArrayJoinClause(ClickHouseParser::ArrayJoinClauseContext *ctx)
{
    return std::make_shared<ArrayJoinClause>(visit(ctx->columnExprList()), !!ctx->LEFT());
}

antlrcpp::Any ParseTreeVisitor::visitPrewhereClause(ClickHouseParser::PrewhereClauseContext *ctx)
{
    return std::make_shared<PrewhereClause>(visit(ctx->columnExpr()).as<PtrTo<ColumnExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitWhereClause(ClickHouseParser::WhereClauseContext *ctx)
{
    return std::make_shared<WhereClause>(visit(ctx->columnExpr()).as<PtrTo<ColumnExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitGroupByClause(ClickHouseParser::GroupByClauseContext *ctx)
{
    return std::make_shared<GroupByClause>(visit(ctx->columnExprList()), !!ctx->TOTALS());
}

antlrcpp::Any ParseTreeVisitor::visitHavingClause(ClickHouseParser::HavingClauseContext *ctx)
{
    return std::make_shared<HavingClause>(visit(ctx->columnExpr()).as<PtrTo<ColumnExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitOrderByClause(ClickHouseParser::OrderByClauseContext *ctx)
{
    return std::make_shared<OrderByClause>(visit(ctx->orderExprList()).as<PtrTo<OrderExprList>>());
}

antlrcpp::Any ParseTreeVisitor::visitLimitByClause(ClickHouseParser::LimitByClauseContext *ctx)
{
    return std::make_shared<LimitByClause>(visit(ctx->limitExpr()), visit(ctx->columnExprList()));
}

antlrcpp::Any ParseTreeVisitor::visitLimitClause(ClickHouseParser::LimitClauseContext *ctx)
{
    return std::make_shared<LimitClause>(visit(ctx->limitExpr()).as<PtrTo<LimitExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitSettingsClause(ClickHouseParser::SettingsClauseContext *ctx)
{
    return std::make_shared<SettingsClause>(visit(ctx->settingExprList()).as<PtrTo<SettingExprList>>());
}

antlrcpp::Any ParseTreeVisitor::visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx)
{
    auto select_stmt = std::make_shared<SelectStmt>(visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>());

    if (ctx->withClause()) select_stmt->setWithClause(visit(ctx->withClause()));
    if (ctx->fromClause()) select_stmt->setFromClause(visit(ctx->fromClause()));
    if (ctx->sampleClause()) select_stmt->setSampleClause(visit(ctx->sampleClause()));
    if (ctx->arrayJoinClause()) select_stmt->setArrayJoinClause(visit(ctx->arrayJoinClause()));
    if (ctx->prewhereClause()) select_stmt->setPrewhereClause(visit(ctx->prewhereClause()));
    if (ctx->whereClause()) select_stmt->setWhereClause(visit(ctx->whereClause()));
    if (ctx->groupByClause()) select_stmt->setGroupByClause(visit(ctx->groupByClause()));
    if (ctx->havingClause()) select_stmt->setHavingClause(visit(ctx->havingClause()));
    if (ctx->orderByClause()) select_stmt->setOrderByClause(visit(ctx->orderByClause()));
    if (ctx->limitByClause()) select_stmt->setLimitByClause(visit(ctx->limitByClause()));
    if (ctx->limitClause()) select_stmt->setLimitClause(visit(ctx->limitClause()));
    if (ctx->settingsClause()) select_stmt->setSettingsClause(visit(ctx->settingsClause()));

    return select_stmt;
}

antlrcpp::Any ParseTreeVisitor::visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx)
{
    auto select_union_query = std::make_shared<SelectUnionQuery>();
    for (auto * stmt : ctx->selectStmt()) select_union_query->appendSelect(visit(stmt));
    return select_union_query;
}

}
