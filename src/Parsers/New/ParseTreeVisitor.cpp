#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/New/AST/SelectStmt.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include "Parsers/New/ClickHouseLexer.h"
#include "Parsers/New/ClickHouseParser.h"


namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitQueryList(ClickHouseParser::QueryListContext *ctx)
{
    auto query_list = std::make_shared<AST::QueryList>();

    for (auto * query : ctx->queryStmt())
        query_list->append(query->accept(this));

    return query_list;
}

antlrcpp::Any ParseTreeVisitor::visitQueryStmt(ClickHouseParser::QueryStmtContext *ctx)
{
    AST::PtrTo<AST::Query> query = ctx->query()->accept(this);

    if (ctx->OUTFILE()) query->setOutFile(ctx->STRING_LITERAL()->accept(this));
    if (ctx->FORMAT()) query->setFormat(ctx->identifier()->accept(this));

    return query;
}

antlrcpp::Any ParseTreeVisitor::visitQuery(ClickHouseParser::QueryContext *ctx)
{
    return ctx->children[0]->accept(this);
}

antlrcpp::Any ParseTreeVisitor::visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx)
{
    auto select_union_query = std::make_shared<AST::SelectUnionQuery>();

    for (auto * stmt : ctx->selectStmt())
        select_union_query->appendSelect(stmt->accept(this));

    return select_union_query;
}

antlrcpp::Any ParseTreeVisitor::visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx)
{
    auto select_stmt = std::make_shared<AST::SelectStmt>(ctx->columnExprList()->accept(this).as<AST::PtrTo<AST::ColumnExprList>>());

    if (ctx->withClause()) select_stmt->setWithClause(ctx->withClause()->accept(this));
    if (ctx->fromClause()) select_stmt->setFromClause(ctx->fromClause()->accept(this));
    if (ctx->sampleClause()) select_stmt->setSampleClause(ctx->sampleClause()->accept(this));
    if (ctx->arrayJoinClause()) select_stmt->setArrayJoinClause(ctx->arrayJoinClause()->accept(this));
    if (ctx->prewhereClause()) select_stmt->setPrewhereClause(ctx->prewhereClause()->accept(this));
    if (ctx->whereClause()) select_stmt->setWhereClause(ctx->whereClause()->accept(this));
    if (ctx->groupByClause()) select_stmt->setGroupByClause(ctx->groupByClause()->accept(this));
    if (ctx->havingClause()) select_stmt->setHavingClause(ctx->havingClause()->accept(this));
    if (ctx->orderByClause()) select_stmt->setOrderByClause(ctx->orderByClause()->accept(this));
    if (ctx->limitByClause()) select_stmt->setLimitByClause(ctx->limitByClause()->accept(this));
    if (ctx->limitClause()) select_stmt->setLimitClause(ctx->limitClause()->accept(this));
    if (ctx->settingsClause()) select_stmt->setSettingsClause(ctx->settingsClause()->accept(this));

    return select_stmt;
}

void ParseTreeVisitor::visitQueryStmtAsParent(AST::Query *query, ClickHouseParser::QueryStmtContext *ctx)
{

}

}
