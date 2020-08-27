#include <Parsers/New/AST/LimitExpr.h>

#include <Parsers/New/AST/Literal.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

LimitExpr::LimitExpr(PtrTo<NumberLiteral> limit, PtrTo<NumberLiteral> offset)
{
    children.push_back(limit);
    children.push_back(offset);
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitLimitExpr(ClickHouseParser::LimitExprContext *ctx)
{
    if (ctx->OFFSET())
        return std::make_shared<LimitExpr>(Literal::createNumber(ctx->INTEGER_LITERAL(0)), Literal::createNumber(ctx->INTEGER_LITERAL(1)));
    else
        return std::make_shared<LimitExpr>(Literal::createNumber(ctx->INTEGER_LITERAL(0)));
}

}
