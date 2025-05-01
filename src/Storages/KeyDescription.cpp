#include <Storages/KeyDescription.h>

#include <Functions/IFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/quoteString.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_USED_IN_KEY;
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
        expression = other.expression->clone();
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
        expression = other.expression->clone();
    else
        expression.reset();

    sample_block = other.sample_block;
    column_names = other.column_names;
    data_types = other.data_types;

    /// additional_column is constant property It should never be lost.
    if (additional_column.has_value() && !other.additional_column.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong key assignment, losing additional_column");
    additional_column = other.additional_column;
    return *this;
}


void KeyDescription::recalculateWithNewAST(
    const ASTPtr & new_ast,
    const ColumnsDescription & columns,
    ContextPtr context)
{
    *this = getSortingKeyFromAST(new_ast, columns, context, additional_column);
}

void KeyDescription::recalculateWithNewColumns(
    const ColumnsDescription & new_columns,
    ContextPtr context)
{
    *this = getSortingKeyFromAST(definition_ast, new_columns, context, additional_column);
}

KeyDescription KeyDescription::getKeyFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    ContextPtr context)
{
    return getSortingKeyFromAST(definition_ast, columns, context, {});
}

bool KeyDescription::moduloToModuloLegacyRecursive(ASTPtr node_expr)
{
    if (!node_expr)
        return false;

    auto * function_expr = node_expr->as<ASTFunction>();
    bool modulo_in_ast = false;
    if (function_expr)
    {
        if (function_expr->name == "modulo")
        {
            function_expr->name = "moduloLegacy";
            modulo_in_ast = true;
        }
        if (function_expr->arguments)
        {
            auto children = function_expr->arguments->children;
            for (const auto & child : children)
                modulo_in_ast |= moduloToModuloLegacyRecursive(child);
        }
    }
    return modulo_in_ast;
}

KeyDescription KeyDescription::getSortingKeyFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    ContextPtr context,
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
    {
        result.data_types.emplace_back(result.sample_block.getByPosition(i).type);
        if (!result.data_types.back()->isComparable())
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_KEY,
                            "Column {} with type {} is not allowed in key expression, it's not comparable",
                            backQuote(result.sample_block.getByPosition(i).name), result.data_types.back()->getName());
    }

    return result;
}

KeyDescription KeyDescription::buildEmptyKey()
{
    KeyDescription result;
    result.expression_list_ast = std::make_shared<ASTExpressionList>();
    result.expression = std::make_shared<ExpressionActions>(ActionsDAG(), ExpressionActionsSettings{});
    return result;
}

KeyDescription KeyDescription::parse(const String & str, const ColumnsDescription & columns, ContextPtr context)
{
    KeyDescription result;
    if (str.empty())
        return result;

    ParserExpression parser;
    ASTPtr ast = parseQuery(parser, "(" + str + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    FunctionNameNormalizer::visit(ast.get());

    return getKeyFromAST(ast, columns, context);
}

}
