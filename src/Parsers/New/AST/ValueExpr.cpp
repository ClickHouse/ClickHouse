#include <Parsers/New/AST/ValueExpr.h>

#include <Parsers/New/AST/Literal.h>

#include <Parsers/New/ParseTreeVisitor.h>
#include "Parsers/New/AST/fwd_decl.h"


namespace DB::AST
{

// static
PtrTo<ValueExpr> ValueExpr::createArray(PtrTo<ValueExprList> array)
{
    PtrList exprs;
    if (array) exprs = {array->begin(), array->end()};
    return PtrTo<ValueExpr>(new ValueExpr(ExprType::ARRAY, exprs));
}

// static
PtrTo<ValueExpr> ValueExpr::createLiteral(PtrTo<Literal> literal)
{
    return PtrTo<ValueExpr>(new ValueExpr(ExprType::LITERAL, {literal}));
}

// static
PtrTo<ValueExpr> ValueExpr::createTuple(PtrTo<ValueExprList> tuple)
{
    return PtrTo<ValueExpr>(new ValueExpr(ExprType::TUPLE, {tuple->begin(), tuple->end()}));
}

ValueExpr::ValueExpr(ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;

    (void)expr_type; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitValueExprArray(ClickHouseParser::ValueExprArrayContext *ctx)
{
    auto list = ctx->valueExprList() ? visit(ctx->valueExprList()).as<PtrTo<ValueExprList>>() : nullptr;
    return ValueExpr::createArray(list);
}

antlrcpp::Any ParseTreeVisitor::visitValueExprList(ClickHouseParser::ValueExprListContext *ctx)
{
    auto list = std::make_shared<ValueExprList>();
    for (auto * expr : ctx->valueExpr()) list->append(visit(expr));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitValueExprLiteral(ClickHouseParser::ValueExprLiteralContext *ctx)
{
    return ValueExpr::createLiteral(visit(ctx->literal()));
}

antlrcpp::Any ParseTreeVisitor::visitValueExprTuple(ClickHouseParser::ValueExprTupleContext *ctx)
{
    return ValueExpr::createTuple(visit(ctx->valueTupleExpr()));
}

}
