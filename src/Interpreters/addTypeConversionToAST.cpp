#include "addTypeConversionToAST.h"

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_DEFAULT_VALUE;
}

ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name)
{
    auto func = makeASTFunction("_CAST", ast, std::make_shared<ASTLiteral>(type_name));

    if (ASTWithAlias * ast_with_alias = dynamic_cast<ASTWithAlias *>(ast.get()))
    {
        func->alias = ast_with_alias->alias;
        func->prefer_alias_to_column_name = ast_with_alias->prefer_alias_to_column_name;
        ast_with_alias->alias.clear();
    }

    return func;
}

ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name, const NamesAndTypesList & all_columns, ContextPtr context)
{
    auto syntax_analyzer_result = TreeRewriter(context).analyze(ast, all_columns);
    const auto actions = ExpressionAnalyzer(ast,
        syntax_analyzer_result,
        const_pointer_cast<Context>(context)).getActions(true);

    for (const auto & action : actions->getActions())
        if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            throw Exception("Unsupported default value that requires ARRAY JOIN action", ErrorCodes::THERE_IS_NO_DEFAULT_VALUE);

    auto block = actions->getSampleBlock();

    auto desc_type =  block.getByName(ast->getAliasOrColumnName()).type;
    if (desc_type->getName() != type_name)
        return addTypeConversionToAST(std::move(ast), type_name);

    return std::move(ast);
}


}
