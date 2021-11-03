#include <Parsers/New/AST/ExplainQuery.h>

#include <Parsers/ASTExplainQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

ExplainQuery::ExplainQuery(PtrTo<Query> query) : Query{query}
{
}

ASTPtr ExplainQuery::convertToOld() const
{
    auto query = std::make_shared<ASTExplainQuery>(ASTExplainQuery::AnalyzedSyntax);

    query->setExplainedQuery(get(QUERY)->convertToOld());

    return query;
}

}

namespace DB
{

using namespace DB::AST;

antlrcpp::Any ParseTreeVisitor::visitExplainStmt(ClickHouseParser::ExplainStmtContext *ctx)
{
    return std::make_shared<ExplainQuery>(visit(ctx->query()).as<PtrTo<Query>>());
}

}
