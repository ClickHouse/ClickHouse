#include <Storages/KeyDescription.h>

#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Storages/extractKeyExpressionList.h>


namespace DB
{

KeyDescription::KeyDescription(const KeyDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , expression(other.expression)
    , sample_block(other.sample_block)
    , column_names(other.column_names)
    , data_types(other.data_types)
{
}

KeyDescription & KeyDescription::operator=(const KeyDescription & other)
{
    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    if (other.expression_list_ast)
        expression_list_ast = other.expression_list_ast->clone();
    expression = other.expression;
    sample_block = other.sample_block;
    column_names = other.column_names;
    data_types = other.data_types;
    return *this;
}


KeyDescription KeyDescription::getKeyFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context, ASTPtr additional_key_expression)
{
    KeyDescription result;
    result.definition_ast = definition_ast;
    result.expression_list_ast = extractKeyExpressionList(definition_ast);

    if (additional_key_expression)
        result.expression_list_ast->children.push_back(additional_key_expression);

    const auto & children = result.expression_list_ast->children;
    for (const auto & child : children)
        result.column_names.emplace_back(child->getColumnName());

    {
        auto expr = result.expression_list_ast->clone();
        auto syntax_result = SyntaxAnalyzer(context).analyze(expr, columns.getAllPhysical());
        /// In expression we also need to store source columns
        result.expression = ExpressionAnalyzer(expr, syntax_result, context).getActions(false);
        /// In sample block we use just key columns
        result.sample_block = ExpressionAnalyzer(expr, syntax_result, context).getActions(true)->getSampleBlock();
    }

    for (size_t i = 0; i < result.sample_block.columns(); ++i)
        result.data_types.emplace_back(result.sample_block.getByPosition(i).type);

    return result;
}

}
