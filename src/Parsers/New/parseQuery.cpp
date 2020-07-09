#include "parseQuery.h"

#include <Parsers/New/AST/SelectStmt.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ClickHouseLexer.h>
#include <Parsers/New/ClickHouseParser.h>

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>


namespace DB
{

antlrcpp::Any ParserTreeVisitor::visitQueryList(ClickHouseParser::QueryListContext *ctx)
{
    auto query_list = std::make_shared<AST::QueryList>();

    for (auto * query : ctx->queryStmt())
    {
        query_list->append(query->accept(this));
    }

    return query_list;
}

antlrcpp::Any ParserTreeVisitor::visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx)
{
    auto select_union_query = std::make_shared<AST::SelectUnionQuery>();

    for (auto * stmt : ctx->selectStmt())
    {
        select_union_query->appendSelect(stmt->accept(this));
    }

    visitQueryStmtAsParent(select_union_query.get(), static_cast<ClickHouseParser::QueryStmtContext*>(ctx->parent));

    return select_union_query;
}

antlrcpp::Any ParserTreeVisitor::visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx)
{
    auto select_stmt = std::make_shared<AST::SelectStmt>();

    select_stmt->setWithClause(ctx->withClause()->accept(this));
    select_stmt->setFromClause(ctx->fromClause()->accept(this));
    select_stmt->setSampleClause(ctx->sampleClause()->accept(this));
    select_stmt->setArrayJoinClause(ctx->arrayJoinClause()->accept(this));
    select_stmt->setPrewhereClause(ctx->prewhereClause()->accept(this));
    select_stmt->setWhereClause(ctx->whereClause()->accept(this));
    select_stmt->setGroupByClause(ctx->groupByClause()->accept(this));
    select_stmt->setHavingClause(ctx->havingClause()->accept(this));
    select_stmt->setOrderByClause(ctx->orderByClause()->accept(this));
    select_stmt->setLimitByClause(ctx->limitByClause()->accept(this));
    select_stmt->setLimitClause(ctx->limitClause()->accept(this));
    select_stmt->setSettingsClause(ctx->settingsClause()->accept(this));

    return select_stmt;
}

ASTPtr parseQuery(const std::string & query)
{
    antlr4::ANTLRInputStream input(query);
    ClickHouseLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);

    ClickHouseParser parser(&tokens);
    ParserTreeVisitor visitor;

    AST::PtrTo<AST::QueryList> new_ast = visitor.visit(parser.queryList());
    return new_ast->begin()->convertToOld();
}

}
