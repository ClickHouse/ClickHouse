#include <Parsers/New/AST/SettingExpr.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

SettingExpr::SettingExpr(PtrTo<Identifier> name, PtrTo<Literal> value)
{
    children.push_back(name);
    children.push_back(value);
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitSettingExprList(ClickHouseParser::SettingExprListContext *ctx)
{
    auto expr_list = std::make_shared<AST::SettingExprList>();
    for (auto* expr : ctx->settingExpr()) expr_list->append(expr->accept(this));
    return expr_list;
}

antlrcpp::Any ParseTreeVisitor::visitSettingExpr(ClickHouseParser::SettingExprContext *ctx)
{
    return std::make_shared<AST::SettingExpr>(visit(ctx->identifier()), visit(ctx->literal()));
}

}
