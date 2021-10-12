#include <Parsers/New/AST/RatioExpr.h>

#include <Parsers/ASTSampleRatio.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

RatioExpr::RatioExpr(PtrTo<NumberLiteral> num1, PtrTo<NumberLiteral> num2) : INode{num1, num2}
{
}

ASTPtr RatioExpr::convertToOld() const
{
    auto numerator = get<NumberLiteral>(NUMERATOR)->convertToOldRational();

    if (has(DENOMINATOR))
    {
        auto denominator = get<NumberLiteral>(DENOMINATOR)->convertToOldRational();

        numerator.numerator = numerator.numerator * denominator.denominator;
        numerator.denominator = numerator.denominator * denominator.numerator;
    }

    return std::make_shared<ASTSampleRatio>(numerator);
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitRatioExpr(ClickHouseParser::RatioExprContext *ctx)
{
    auto denominator = ctx->numberLiteral().size() == 2 ? visit(ctx->numberLiteral(1)).as<PtrTo<NumberLiteral>>() : nullptr;
    return std::make_shared<RatioExpr>(visit(ctx->numberLiteral(0)), denominator);
}

}
