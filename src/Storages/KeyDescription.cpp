#include <Storages/KeyDescription.h>
#include <Storages/VirtualColumnUtils.h>

#include <Functions/IFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/quoteString.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ParserCreateQuery.h>
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
    , reverse_flags(other.reverse_flags)
    , data_types(other.data_types)
    , additional_columns(other.additional_columns)
    , sort_order_id(other.sort_order_id)
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
    reverse_flags = other.reverse_flags;
    data_types = other.data_types;

    if (!additional_columns.empty() && other.additional_columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong key assignment, losing additional_columns");

    additional_columns = other.additional_columns;
    sort_order_id = other.sort_order_id;
    return *this;
}

void KeyDescription::recalculateWithNewAST(
    const ASTPtr & new_ast,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context)
{
    *this = getKeyFromAST(new_ast, columns, virtuals, context, additional_columns);
}

void KeyDescription::recalculateWithNewColumns(
    const ColumnsDescription & new_columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context)
{
    *this = getKeyFromAST(definition_ast, new_columns, virtuals, context, additional_columns);
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

/// Build expression_list_ast, column_names, and reverse_flags from key children and additional columns.
std::tuple<ASTPtr, Names, std::vector<bool>> buildKeyColumns(
    const ASTPtr & key_expression_list,
    const NamesAndTypesList & additional_columns)
{
    auto expression_list_ast = make_intrusive<ASTExpressionList>();
    Names column_names;
    std::vector<bool> reverse_flags;

    for (const auto & child : key_expression_list->children)
    {
        auto real_key = child;
        if (auto * elem = child->as<ASTStorageOrderByElement>())
        {
            real_key = elem->children.front();
            reverse_flags.emplace_back(elem->direction < 0);
        }

        expression_list_ast->children.push_back(real_key);
        column_names.emplace_back(real_key->getColumnName());
    }

    for (const auto & col : additional_columns)
    {
        if (std::ranges::contains(column_names, col.name))
            continue;

        ASTPtr column_identifier = make_intrusive<ASTIdentifier>(col.name);
        column_names.emplace_back(column_identifier->getColumnName());
        expression_list_ast->children.push_back(column_identifier);

        if (!reverse_flags.empty())
            reverse_flags.emplace_back(false);
    }

    return {expression_list_ast, std::move(column_names), std::move(reverse_flags)};
}

KeyDescription KeyDescription::getKeyFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context,
    const NamesAndTypesList & additional_columns)
{
    KeyDescription result;
    result.definition_ast = definition_ast;
    result.additional_columns = additional_columns;
    auto key_expression_list = extractKeyExpressionList(definition_ast);
    checkExpressionDoesntContainSubqueries(*key_expression_list);

    std::tie(result.expression_list_ast, result.column_names, result.reverse_flags) = buildKeyColumns(key_expression_list, additional_columns);
    if (!result.reverse_flags.empty() && result.reverse_flags.size() != result.expression_list_ast->children.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The size of reverse_flags ({}) does not match the size of KeyDescription {}",
            result.reverse_flags.size(), result.expression_list_ast->children.size());

    {
        auto expr = result.expression_list_ast->clone();
        auto all_columns = VirtualColumnUtils::getColumnsWithVirtualsForAnalysis(columns, virtuals);
        auto syntax_result = TreeRewriter(context).analyze(expr, all_columns);
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

        auto check = [&](const IDataType & type)
        {
            if (isDynamic(type) || isVariant(type) || isObject(type))
                throw Exception(
                    ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_KEY,
                    "Column with type Variant/Dynamic/JSON is not allowed in key expression. Consider using a subcolumn with a specific data "
                    "type instead (for example 'column.Int64' or 'json.some.path.:Int64' if its a JSON path subcolumn) or casting this column to a specific data type");
        };

        check(*result.data_types.back());
        result.data_types.back()->forEachChild(check);
    }

    return result;
}

ASTPtr KeyDescription::getOriginalExpressionList() const
{
    if (!expression_list_ast || reverse_flags.empty())
        return expression_list_ast;

    auto expr_list = make_intrusive<ASTExpressionList>();
    size_t size = expression_list_ast->children.size();
    for (size_t i = 0; i < size; ++i)
    {
        auto column_ast = make_intrusive<ASTStorageOrderByElement>();
        column_ast->children.push_back(expression_list_ast->children[i]);
        column_ast->direction = (!reverse_flags.empty() && reverse_flags[i]) ? -1 : 1;
        expr_list->children.push_back(std::move(column_ast));
    }

    return expr_list;
}

KeyDescription KeyDescription::buildEmptyKey()
{
    KeyDescription result;
    result.expression_list_ast = make_intrusive<ASTExpressionList>();
    result.expression = std::make_shared<ExpressionActions>(ActionsDAG(), ExpressionActionsSettings{});
    return result;
}

KeyDescription KeyDescription::parse(
    const String & str,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context,
    bool allow_order)
{
    KeyDescription result;
    if (str.empty())
        return result;

    ParserStorageOrderByClause parser(allow_order);
    ASTPtr ast = parseQuery(parser, "(" + str + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    FunctionNameNormalizer::visit(ast.get());

    return getKeyFromAST(ast, columns, virtuals, context);
}

}
