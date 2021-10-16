#include <Parsers/New/AST/SetQuery.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SettingExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

SetQuery::SetQuery(PtrTo<SettingExprList> list) : Query{list}
{
}

ASTPtr SetQuery::convertToOld() const
{
    auto expr = std::make_shared<ASTSetQuery>();

    for (const auto & child : get(EXPRS)->as<SettingExprList &>())
    {
        const auto * setting = child->as<SettingExpr>();
        expr->changes.emplace_back(setting->getName()->getName(), setting->getValue()->convertToOld()->as<ASTLiteral>()->value);
    }

    return expr;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitSetStmt(ClickHouseParser::SetStmtContext *ctx)
{
    return std::make_shared<SetQuery>(visit(ctx->settingExprList()).as<PtrTo<SettingExprList>>());
}

}
