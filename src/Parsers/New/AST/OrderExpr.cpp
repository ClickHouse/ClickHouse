#include <Parsers/New/AST/OrderExpr.h>

#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

OrderExpr::OrderExpr(PtrTo<ColumnExpr> expr_, NullsOrder nulls_, PtrTo<StringLiteral> collate_, bool ascending)
    : expr(expr_), nulls(nulls_), collate(collate_), asc(ascending)
{
    /// FIXME: remove this.
    (void)nulls;
    (void)asc;
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitOrderExprList(ClickHouseParser::OrderExprListContext *ctx)
{
    auto expr_list = std::make_shared<AST::OrderExprList>();
    for (auto* expr : ctx->orderExpr()) expr_list->append(expr->accept(this));
    return expr_list;
}

antlrcpp::Any ParseTreeVisitor::visitOrderExpr(ClickHouseParser::OrderExprContext *ctx)
{
    AST::OrderExpr::NullsOrder nulls = AST::OrderExpr::NATURAL;
    if (ctx->FIRST()) nulls = AST::OrderExpr::NULLS_FIRST;
    else if (ctx->LAST()) nulls = AST::OrderExpr::NULLS_LAST;

    AST::PtrTo<AST::StringLiteral> collate;
    if (ctx->COLLATE()) collate = AST::Literal::createString(ctx->STRING_LITERAL());

    return std::make_shared<AST::OrderExpr>(ctx->columnExpr()->accept(this), nulls, collate, !ctx->DESCENDING() && !ctx->DESC());
}

}
