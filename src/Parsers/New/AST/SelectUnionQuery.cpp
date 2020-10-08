#include <Parsers/New/AST/SelectUnionQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/JoinExpr.h>
#include <Parsers/New/AST/LimitExpr.h>
#include <Parsers/New/AST/SettingExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>

namespace DB::AST
{

// FROM Clause

FromClause::FromClause(PtrTo<JoinExpr> expr) : INode{expr}
{
}

ASTPtr FromClause::convertToOld() const
{
    auto old_tables = std::make_shared<ASTTablesInSelectQuery>();
    old_tables->children = get(EXPR)->convertToOld()->children;
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

ArrayJoinClause::ArrayJoinClause(PtrTo<ColumnExprList> expr_list, bool left_) : INode{expr_list}, left(left_)
{
}

ASTPtr ArrayJoinClause::convertToOld() const
{
    auto element = std::make_shared<ASTTablesInSelectQueryElement>();
    auto array_join = std::make_shared<ASTArrayJoin>();

    if (left) array_join->kind = ASTArrayJoin::Kind::Left;
    else array_join->kind = ASTArrayJoin::Kind::Inner;

    array_join->expression_list = get(EXPRS)->convertToOld();
    array_join->children.push_back(array_join->expression_list);

    element->array_join = array_join;
    element->children.push_back(element->array_join);

    return element;
}

// LIMIT By Clause

LimitByClause::LimitByClause(PtrTo<LimitExpr> expr, PtrTo<ColumnExprList> expr_list) : limit(expr), by(expr_list)
{
}

// SETTINGS Clause

SettingsClause::SettingsClause(PtrTo<SettingExprList> expr_list) : INode{expr_list}
{
}

ASTPtr SettingsClause::convertToOld() const
{
    auto expr = std::make_shared<ASTSetQuery>();

    for (const auto & child : get(EXPRS)->as<SettingExprList &>())
    {
        const auto * setting = child->as<SettingExpr>();
        expr->changes.emplace_back(setting->getName()->getName(), setting->getValue()->convertToOld()->as<ASTLiteral>()->value);
    }

    return expr;
}

// SELECT Statement

SelectStmt::SelectStmt(bool distinct_, bool with_totals_, PtrTo<ColumnExprList> expr_list)
    : INode(MAX_INDEX), distinct(distinct_), with_totals(with_totals_)
{
    set(COLUMNS, expr_list);
}

void SelectStmt::setWithClause(PtrTo<WithClause> clause)
{
    set(WITH, clause);
}

void SelectStmt::setFromClause(PtrTo<FromClause> clause)
{
    set(FROM, clause);
}

void SelectStmt::setSampleClause(PtrTo<SampleClause> clause)
{
    set(SAMPLE, clause);
}

void SelectStmt::setArrayJoinClause(PtrTo<ArrayJoinClause> clause)
{
    set(ARRAY_JOIN, clause);
}

void SelectStmt::setPrewhereClause(PtrTo<PrewhereClause> clause)
{
    set(PREWHERE, clause);
}

void SelectStmt::setWhereClause(PtrTo<WhereClause> clause)
{
    set(WHERE, clause);
}

void SelectStmt::setGroupByClause(PtrTo<GroupByClause> clause)
{
    set(GROUP_BY, clause);
}

void SelectStmt::setHavingClause(PtrTo<HavingClause> clause)
{
    set(HAVING, clause);
}

void SelectStmt::setOrderByClause(PtrTo<OrderByClause> clause)
{
    set(ORDER_BY, clause);
}

void SelectStmt::setLimitByClause(PtrTo<LimitByClause> clause)
{
    set(LIMIT_BY, clause);
}

void SelectStmt::setLimitClause(PtrTo<LimitClause> clause)
{
    set(LIMIT, clause);
}

void SelectStmt::setSettingsClause(PtrTo<SettingsClause> clause)
{
    set(SETTINGS, clause);
}

ASTPtr SelectStmt::convertToOld() const
{
    auto old_select = std::make_shared<ASTSelectQuery>();

    old_select->setExpression(ASTSelectQuery::Expression::SELECT, get(COLUMNS)->convertToOld());
    old_select->distinct = distinct;
    old_select->group_by_with_totals = with_totals;

    if (has(WITH)) old_select->setExpression(ASTSelectQuery::Expression::WITH, get(WITH)->convertToOld());
    if (has(FROM)) old_select->setExpression(ASTSelectQuery::Expression::TABLES, get(FROM)->convertToOld());
    // TODO: SAMPLE
    if (has(ARRAY_JOIN)) old_select->tables()->children.push_back(get(ARRAY_JOIN)->convertToOld());
    if (has(PREWHERE)) old_select->setExpression(ASTSelectQuery::Expression::PREWHERE, get(PREWHERE)->convertToOld());
    if (has(WHERE)) old_select->setExpression(ASTSelectQuery::Expression::WHERE, get(WHERE)->convertToOld());
    if (has(GROUP_BY)) old_select->setExpression(ASTSelectQuery::Expression::GROUP_BY, get(GROUP_BY)->convertToOld());
    if (has(HAVING)) old_select->setExpression(ASTSelectQuery::Expression::HAVING, get(HAVING)->convertToOld());
    if (has(ORDER_BY)) old_select->setExpression(ASTSelectQuery::Expression::ORDER_BY, get(ORDER_BY)->convertToOld());
    // TODO: LIMIT BY
    if (has(LIMIT))
    {
        auto old_list = get(LIMIT)->convertToOld();
        old_select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(old_list->children[0]));
        if (old_list->children.size() > 1)
            old_select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(old_list->children[1]));
    }
    if (has(SETTINGS)) old_select->setExpression(ASTSelectQuery::Expression::SETTINGS, get(SETTINGS)->convertToOld());

    return old_select;
}

SelectUnionQuery::SelectUnionQuery(PtrTo<List<SelectStmt>> stmts) : Query{stmts}
{
}

void SelectUnionQuery::appendSelect(PtrTo<SelectStmt> stmt)
{
    if (!has(STMTS)) push(std::make_shared<List<SelectStmt>>());
    get<List<SelectStmt>>(STMTS)->push(stmt);
}

void SelectUnionQuery::appendSelect(PtrTo<SelectUnionQuery> query)
{
    for (const auto & stmt : query->get(STMTS)->as<List<SelectStmt> &>())
        appendSelect(static_pointer_cast<SelectStmt>(stmt));
}

ASTPtr SelectUnionQuery::convertToOld() const
{
    auto query = std::make_shared<ASTSelectWithUnionQuery>();

    query->list_of_selects = std::make_shared<ASTExpressionList>();
    query->children.push_back(query->list_of_selects);

    for (const auto & select : get(STMTS)->as<List<SelectStmt> &>())
        query->list_of_selects->children.push_back(select->convertToOld());

    convertToOldPartially(query);

    return query;
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
    return std::make_shared<FromClause>(visit(ctx->joinExpr()).as<PtrTo<JoinExpr>>());
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
    return std::make_shared<GroupByClause>(visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>());
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
    auto select_stmt = std::make_shared<SelectStmt>(!!ctx->DISTINCT(), !!ctx->WITH(), visit(ctx->columnExprList()));

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

antlrcpp::Any ParseTreeVisitor::visitSelectStmtWithParens(ClickHouseParser::SelectStmtWithParensContext *ctx)
{
    PtrTo<SelectUnionQuery> query;

    if (ctx->selectStmt())
    {
        query = std::make_shared<SelectUnionQuery>();
        query->appendSelect(visit(ctx->selectStmt()).as<PtrTo<SelectStmt>>());
    }
    else if (ctx->selectUnionStmt())
    {
         query = visit(ctx->selectUnionStmt());
    }

    return query;
}

antlrcpp::Any ParseTreeVisitor::visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx)
{
    auto select_union_query = std::make_shared<SelectUnionQuery>();
    for (auto * stmt : ctx->selectStmtWithParens()) select_union_query->appendSelect(visit(stmt).as<PtrTo<SelectUnionQuery>>());
    return select_union_query;
}

}
