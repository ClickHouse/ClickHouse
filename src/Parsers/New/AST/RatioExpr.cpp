#include <Parsers/New/AST/RatioExpr.h>

#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

RatioExpr::RatioExpr(PtrTo<NumberLiteral> num) : num1(num)
{
}

RatioExpr::RatioExpr(PtrTo<NumberLiteral> num1_, PtrTo<NumberLiteral> num2_) : num1(num1_), num2(num2_)
{
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitRatioExpr(ClickHouseParser::RatioExprContext *ctx)
{
    /// TODO: not complete!
    if (ctx->INTEGER_LITERAL().size() == 2)
        return std::make_shared<RatioExpr>(Literal::createNumber(ctx->INTEGER_LITERAL(0)), Literal::createNumber(ctx->INTEGER_LITERAL(1)));
    else
        return std::make_shared<RatioExpr>(Literal::createNumber(ctx->INTEGER_LITERAL(0)));
}

}
