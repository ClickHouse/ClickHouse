#include "addTypeConversionToAST.h"

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name)
{
    auto func = makeASTFunction("cast", ast, std::make_shared<ASTLiteral>(type_name));

    if (ASTWithAlias * ast_with_alias = dynamic_cast<ASTWithAlias *>(ast.get()))
    {
        func->alias = ast_with_alias->alias;
        func->prefer_alias_to_column_name = ast_with_alias->prefer_alias_to_column_name;
        ast_with_alias->alias.clear();
    }

    return func;
}

}
