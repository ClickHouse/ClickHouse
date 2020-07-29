#include <stdexcept>
#include <Parsers/New/AST/ColumnExpr.h>

#include <Parsers/New/ClickHouseParser.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<ColumnExpr> ColumnExpr::createLiteral(PtrTo<Literal> literal)
{
    return PtrTo<ColumnExpr>(new ColumnExpr(ExprType::LITERAL, {literal}));
}

ColumnExpr::ColumnExpr(ColumnExpr::ExprType type, std::vector<Ptr> exprs) : expr_type(type)
{
    children = exprs;
}

ASTPtr ColumnExpr::convertToOld() const
{
    switch (expr_type)
    {
        case ExprType::LITERAL:
            return children[LITERAL]->convertToOld();
        default:
            throw std::logic_error("Unsupported type of column expression");
    }
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *ctx)
{
    return AST::ColumnExpr::createLiteral(std::make_shared<AST::Literal>(ctx->LITERAL()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnExprList(ClickHouseParser::ColumnExprListContext *ctx)
{
    auto expr_list = std::make_shared<AST::ColumnExprList>();
    for (auto* expr : ctx->columnExpr()) expr_list->append(expr->accept(this));
    return expr_list;
}

}
