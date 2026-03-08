#include <Interpreters/addTypeConversionToAST.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Planner/AnalyzeExpression.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_DEFAULT_VALUE;
}

ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name)
{
    auto func = makeASTFunction("_CAST", ast, make_intrusive<ASTLiteral>(type_name));

    if (ASTWithAlias * ast_with_alias = dynamic_cast<ASTWithAlias *>(ast.get()))
    {
        func->alias = ast_with_alias->alias;
        func->setPreferAliasToColumnName(ast_with_alias->preferAliasToColumnName());
        ast_with_alias->alias.clear();
    }

    return func;
}

ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name, const NamesAndTypesList & all_columns, ContextPtr context)
{
    const auto actions = analyzeExpressionToActions(ast, all_columns, context, true);

    for (const auto & action : actions->getActions())
        if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            throw Exception(ErrorCodes::THERE_IS_NO_DEFAULT_VALUE, "Unsupported default value that requires ARRAY JOIN action");

    auto block = actions->getSampleBlock();

    auto desc_type =  block.getByName(ast->getAliasOrColumnName()).type;
    if (desc_type->getName() != type_name)
        return addTypeConversionToAST(std::move(ast), type_name);

    return std::move(ast);
}


}
