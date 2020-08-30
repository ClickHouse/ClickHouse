#include <Parsers/New/AST/RatioExpr.h>

#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

RatioExpr::RatioExpr(PtrTo<NumberLiteral> num1, PtrTo<NumberLiteral> num2)
{
    children.push_back(num1);
    children.push_back(num2);
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitRatioExpr(ClickHouseParser::RatioExprContext *ctx)
{
    if (ctx->numberLiteral().size() == 2)
        return std::make_shared<RatioExpr>(visit(ctx->numberLiteral(0)), visit(ctx->numberLiteral(1)));
    else
        return std::make_shared<RatioExpr>(visit(ctx->numberLiteral(0)).as<PtrTo<NumberLiteral>>());
}

}
