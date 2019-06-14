#include "addTypeConversionToAST.h"

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name)
{
    auto func = std::make_shared<ASTFunction>();
    ASTPtr res = func;

    if (ASTWithAlias * ast_with_alias = ast->as<ASTWithAlias>())
    {
        func->alias = ast_with_alias->alias;
        func->prefer_alias_to_column_name = ast_with_alias->prefer_alias_to_column_name;
        ast_with_alias->alias.clear();
    }

    func->name = "CAST";
    auto exp_list = std::make_shared<ASTExpressionList>();
    func->arguments = exp_list;
    func->children.push_back(func->arguments);
    exp_list->children.emplace_back(std::move(ast));
    exp_list->children.emplace_back(std::make_shared<ASTLiteral>(type_name));
    return res;
}

}
