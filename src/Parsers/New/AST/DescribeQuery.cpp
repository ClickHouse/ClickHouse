#include <Parsers/New/AST/DescribeQuery.h>

#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

DescribeQuery::DescribeQuery(PtrTo<TableExpr> expr)
{
    children.push_back(expr);
}

ASTPtr DescribeQuery::convertToOld() const
{
    auto query = std::make_shared<ASTDescribeQuery>();

    // TODO

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitDescribeStmt(ClickHouseParser::DescribeStmtContext *ctx)
{
    return std::make_shared<DescribeQuery>(visit(ctx->tableExpr()).as<PtrTo<TableExpr>>());
}

}
