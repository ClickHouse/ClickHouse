#include <Databases/NormalizeAndEvaluateConstantsVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>


namespace DB
{

void NormalizeAndEvaluateConstants::visit(const ASTPtr & ast, Data & data)
{
    assert(data.create_query_context->hasQueryContext());

    /// Looking for functions in column default expressions and dictionary source definition
    if (const auto * function = ast->as<ASTFunction>())
        visit(*function, data);
    else if (const auto * dict_source = ast->as<ASTFunctionWithKeyValueArguments>())
        visit(*dict_source, data);
}

void NormalizeAndEvaluateConstants::visit(const ASTFunction & function, Data & data)
{
    /// Replace expressions like "dictGet(currentDatabase() || '.dict', 'value', toUInt32(1))"
    /// with "dictGet('db_name.dict', 'value', toUInt32(1))"
    ssize_t table_name_arg_idx = getPositionOfTableNameArgumentToEvaluate(function);
    if (table_name_arg_idx < 0)
        return;

    if (!function.arguments || function.arguments->children.size() <= static_cast<size_t>(table_name_arg_idx))
        return;

    auto & arg = function.arguments->as<ASTExpressionList &>().children[table_name_arg_idx];
    if (arg->as<ASTFunction>())
        arg = evaluateConstantExpressionAsLiteral(arg, data.create_query_context);
}


void NormalizeAndEvaluateConstants::visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data)
{
    if (!dict_source.elements)
        return;

    auto & expr_list = dict_source.elements->as<ASTExpressionList &>();
    for (auto & child : expr_list.children)
    {
        ASTPair * pair = child->as<ASTPair>();
        if (pair->second->as<ASTFunction>())
        {
            auto ast_literal = evaluateConstantExpressionAsLiteral(pair->children[0], data.create_query_context);
            pair->replace(pair->second, ast_literal);
        }
    }
}

}
