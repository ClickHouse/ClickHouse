#include <Parsers/New/AST/OrderExpr.h>

#include <Parsers/ASTOrderByElement.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

OrderExpr::OrderExpr(PtrTo<ColumnExpr> expr, NullsOrder nulls_, PtrTo<StringLiteral> collate, bool ascending)
    : INode{expr, collate}, nulls(nulls_), asc(ascending)
{
}

ASTPtr OrderExpr::convertToOld() const
{
    auto expr = std::make_shared<ASTOrderByElement>();

    expr->children.push_back(get(EXPR)->convertToOld());
    expr->direction = asc ? 1 : -1;
    expr->nulls_direction_was_explicitly_specified = (nulls != NATURAL);
    if (nulls == NATURAL) expr->nulls_direction = expr->direction;
    else expr->nulls_direction = (nulls == NULLS_LAST) ? expr->direction : -expr->direction;

    if (has(COLLATE))
    {
        expr->collation = get(COLLATE)->convertToOld();
        expr->children.push_back(expr->collation);
    }

    // TODO: WITH FILL?

    return expr;
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitOrderExprList(ClickHouseParser::OrderExprListContext *ctx)
{
    auto expr_list = std::make_shared<AST::OrderExprList>();
    for (auto* expr : ctx->orderExpr()) expr_list->push(visit(expr));
    return expr_list;
}

antlrcpp::Any ParseTreeVisitor::visitOrderExpr(ClickHouseParser::OrderExprContext *ctx)
{
    AST::OrderExpr::NullsOrder nulls = AST::OrderExpr::NATURAL;
    if (ctx->FIRST()) nulls = AST::OrderExpr::NULLS_FIRST;
    else if (ctx->LAST()) nulls = AST::OrderExpr::NULLS_LAST;

    AST::PtrTo<AST::StringLiteral> collate;
    if (ctx->COLLATE()) collate = AST::Literal::createString(ctx->STRING_LITERAL());

    return std::make_shared<AST::OrderExpr>(visit(ctx->columnExpr()), nulls, collate, !ctx->DESCENDING() && !ctx->DESC());
}

}
