#include <Storages/KeyDescription.h>

#include <Functions/IFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/extractKeyExpressionList.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

KeyDescription::KeyDescription(const KeyDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , sample_block(other.sample_block)
    , column_names(other.column_names)
    , data_types(other.data_types)
    , additional_column(other.additional_column)
{
    if (other.expression)
        expression = std::make_shared<ExpressionActions>(*other.expression);
}

KeyDescription & KeyDescription::operator=(const KeyDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    if (other.expression_list_ast)
        expression_list_ast = other.expression_list_ast->clone();
    else
        expression_list_ast.reset();


    if (other.expression)
        expression = std::make_shared<ExpressionActions>(*other.expression);
    else
        expression.reset();

    sample_block = other.sample_block;
    column_names = other.column_names;
    data_types = other.data_types;

    /// additional_column is constant property It should never be lost.
    if (additional_column.has_value() && !other.additional_column.has_value())
        throw Exception("Wrong key assignment, loosing additional_column", ErrorCodes::LOGICAL_ERROR);
    additional_column = other.additional_column;
    return *this;
}


void KeyDescription::recalculateWithNewAST(
    const ASTPtr & new_ast,
    const ColumnsDescription & columns,
    const Context & context)
{
    *this = getSortingKeyFromAST(new_ast, columns, context, additional_column);
}

void KeyDescription::recalculateWithNewColumns(
    const ColumnsDescription & new_columns,
    const Context & context)
{
    *this = getSortingKeyFromAST(definition_ast, new_columns, context, additional_column);
}

KeyDescription KeyDescription::getKeyFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    const Context & context)
{
    return getSortingKeyFromAST(definition_ast, columns, context, {});
}

KeyDescription KeyDescription::getSortingKeyFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    const Context & context,
    const std::optional<String> & additional_column)
{
    KeyDescription result;
    result.definition_ast = definition_ast;
    result.expression_list_ast = extractKeyExpressionList(definition_ast);

    if (additional_column)
    {
        result.additional_column = additional_column;
        ASTPtr column_identifier = std::make_shared<ASTIdentifier>(*additional_column);
        result.expression_list_ast->children.push_back(column_identifier);
    }

    const auto & children = result.expression_list_ast->children;
    for (const auto & child : children)
        result.column_names.emplace_back(child->getColumnName());

    {
        auto expr = result.expression_list_ast->clone();
        auto syntax_result = TreeRewriter(context).analyze(expr, columns.getAllPhysical());
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
