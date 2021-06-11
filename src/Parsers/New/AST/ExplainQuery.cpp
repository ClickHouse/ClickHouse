#include <Parsers/New/AST/ExplainQuery.h>

#include <Parsers/ASTExplainQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<ExplainQuery> ExplainQuery::createExplainAST(PtrTo<Query> query)
{
    return PtrTo<ExplainQuery>(new ExplainQuery(QueryType::AST, {query}));
}

// static
PtrTo<ExplainQuery> ExplainQuery::createExplainSyntax(PtrTo<Query> query)
{
    return PtrTo<ExplainQuery>(new ExplainQuery(QueryType::SYNTAX, {query}));
}

ExplainQuery::ExplainQuery(QueryType type, PtrList exprs) : Query{exprs}, query_type(type)
{
}

ASTPtr ExplainQuery::convertToOld() const
{
    ASTPtr query;

    switch (query_type)
    {
        case QueryType::AST:
            query = std::make_shared<ASTExplainQuery>(ASTExplainQuery::ParsedAST);
            break;
        case QueryType::SYNTAX:
            query = std::make_shared<ASTExplainQuery>(ASTExplainQuery::AnalyzedSyntax);
            break;
    }

    query->as<ASTExplainQuery>()->setExplainedQuery(get(QUERY)->convertToOld());

    return query;
}

}

namespace DB
{

using namespace DB::AST;

antlrcpp::Any ParseTreeVisitor::visitExplainASTStmt(ClickHouseParser::ExplainASTStmtContext *ctx)
{
    return ExplainQuery::createExplainAST(visit(ctx->query()).as<PtrTo<Query>>());
}

antlrcpp::Any ParseTreeVisitor::visitExplainSyntaxStmt(ClickHouseParser::ExplainSyntaxStmtContext *ctx)
{
    return ExplainQuery::createExplainSyntax(visit(ctx->query()).as<PtrTo<Query>>());
}

}
