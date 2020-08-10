#include <Parsers/New/AST/SetQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SettingExpr.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>


namespace DB::AST
{

SetQuery::SetQuery(PtrTo<SettingExpr> expr)
{
    children.push_back(expr);
}

ASTPtr SetQuery::convertToOld() const
{
    auto expr = std::make_shared<ASTSetQuery>();
    const auto * setting = children[0]->as<SettingExpr>();

    expr->changes.emplace_back(setting->getName()->getName(), setting->getValue()->convertToOld()->as<ASTLiteral>()->value);

    return expr;
}

}
