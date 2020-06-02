#include "parseQuery.h"

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

antlrcpp::Any ParserTreeVisitor::visitQueryList(ClickHouseParser::QueryListContext *ctx)
{
    std::vector<ASTPtr> query_list;

    for (auto * query : ctx->queryStmt())
    {
        query_list.push_back(query->accept(this));
    }

    return query_list;
}

antlrcpp::Any ParserTreeVisitor::visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx)
{
    ASTPtr select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

    for (auto * stmt : ctx->selectStmt())
    {
        select_with_union_query->list_of_selects->as<ASTExpressionList>()->children.push_back(stmt->accept(this));
    }

    return select_with_union_query;
}

antlrcpp::Any ParserTreeVisitor::visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx)
{
    using Expression = ASTSelectQuery::Expression;

    std::shared_ptr<ASTSelectQuery> select_stmt = std::make_shared<ASTSelectQuery>();

    select_stmt->setExpression(Expression::WITH, ctx->withClause()->accept(this));
    select_stmt->setExpression(Expression::TABLES, ctx->fromClause()->accept(this));
    // TODO: sampleClause
    // TODO: arrayJoin
    select_stmt->setExpression(Expression::PREWHERE, ctx->prewhereClause()->accept(this));
    select_stmt->setExpression(Expression::WHERE, ctx->whereClause()->accept(this));
    select_stmt->setExpression(Expression::GROUP_BY, ctx->groupByClause()->accept(this));
    select_stmt->setExpression(Expression::HAVING, ctx->havingClause()->accept(this));
    select_stmt->setExpression(Expression::ORDER_BY, ctx->orderByClause()->accept(this));
    select_stmt->setExpression(Expression::LIMIT_BY, ctx->limitByClause()->accept(this));
    select_stmt->setExpression(Expression::LIMIT, ctx->limitClause()->accept(this));
    select_stmt->setExpression(Expression::SETTINGS, ctx->settingsClause()->accept(this));

    return select_stmt;
}

}
