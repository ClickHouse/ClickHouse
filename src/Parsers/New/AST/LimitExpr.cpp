#include <Parsers/New/AST/LimitExpr.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

LimitExpr::LimitExpr(PtrTo<NumberLiteral> limit_) : limit(limit_)
{
}

LimitExpr::LimitExpr(PtrTo<NumberLiteral> limit_, PtrTo<NumberLiteral> offset_) : limit(limit_), offset(offset_)
{
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitLimitExpr(ClickHouseParser::LimitExprContext *ctx)
{
    if (ctx->OFFSET()) return std::make_shared<AST::LimitExpr>(ctx->NUMBER_LITERAL(0)->accept(this), ctx->NUMBER_LITERAL(1)->accept(this));
    else return std::make_shared<AST::LimitExpr>(ctx->NUMBER_LITERAL(0)->accept(this));
}

}
