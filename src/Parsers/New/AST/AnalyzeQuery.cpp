#include <Parsers/New/AST/AnalyzeQuery.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

AnalyzeQuery::AnalyzeQuery(PtrTo<Query> query)
{
    children.push_back(query);
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitAnalyzeStmt(ClickHouseParser::AnalyzeStmtContext *ctx)
{
    return std::make_shared<AnalyzeQuery>(visit(ctx->queryStmt()).as<PtrTo<Query>>());
}

}
