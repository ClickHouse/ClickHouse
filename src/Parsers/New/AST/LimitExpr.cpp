#include <Parsers/New/AST/LimitExpr.h>

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

LimitExpr::LimitExpr(PtrTo<ColumnExpr> limit, PtrTo<ColumnExpr> offset) : INode{limit, offset}
{
}

ASTPtr LimitExpr::convertToOld() const
{
    auto list = std::make_shared<ASTExpressionList>();

    if (has(OFFSET)) list->children.push_back(get(OFFSET)->convertToOld());
    list->children.push_back(get(LIMIT)->convertToOld());

    return list;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitLimitExpr(ClickHouseParser::LimitExprContext *ctx)
{
    if (ctx->columnExpr().size() == 2)
        return std::make_shared<LimitExpr>(visit(ctx->columnExpr(0)), visit(ctx->columnExpr(1)));
    else
        return std::make_shared<LimitExpr>(visit(ctx->columnExpr(0)).as<PtrTo<ColumnExpr>>());
}

}
