#include <Parsers/New/AST/RatioExpr.h>

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

antlrcpp::Any ParseTreeVisitor::visitRatioExpr(ClickHouseParser::RatioExprContext *ctx)
{
    /// TODO: not complete!
    if (ctx->SLASH()) return std::make_shared<AST::RatioExpr>(ctx->NUMBER_LITERAL(0)->accept(this), ctx->NUMBER_LITERAL(1)->accept(this));
    else return std::make_shared<AST::RatioExpr>(ctx->NUMBER_LITERAL(0)->accept(this));
}

}
