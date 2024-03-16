#include <ranges>

#include <Functions/IFunction.h>

#include <Common/quoteString.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/TreeRewriter.h>

#include <Storages/KeyDescription.h>
#include <Storages/extractKeyExpressionList.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_USED_IN_KEY;
}

/// settings extenders
KeyDescription::Builder::Builder(std::optional<AdditionalSettings> settings_) : settings{std::move(settings_)}
{
}

KeyDescription::Builder & KeyDescription::Builder::withExtFrontColumns(std::vector<String> ext_columns_front_)
{
    if (!settings.has_value())
        settings = AdditionalSettings{};

    settings->ext_columns_front = std::move(ext_columns_front_);

    return *this;
}

KeyDescription::Builder & KeyDescription::Builder::withExtBackColumns(std::vector<String> ext_columns_back_)
{
    if (!settings.has_value())
        settings = AdditionalSettings{};

    settings->ext_columns_back = std::move(ext_columns_back_);

    return *this;
}

KeyDescription KeyDescription::Builder::buildEmpty()
{
    chassert(!settings.has_value());

    KeyDescription result;
    result.expression_list_ast = std::make_shared<ASTExpressionList>();
    result.expression = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(), ExpressionActionsSettings{});

    return result;
}

KeyDescription
KeyDescription::Builder::buildFromAST(const ASTPtr & definition_ast_, const ColumnsDescription & columns_, ContextPtr context_)
{
    KeyDescription result;
    result.definition_ast = definition_ast_;
    result.expression_list_ast = extractKeyExpressionList(definition_ast_);
    result.additional_settings = settings;

    if (settings.has_value())
    {
        for (const auto & additional_column : std::ranges::views::reverse(settings->ext_columns_front))
        {
            ASTPtr column_identifier = std::make_shared<ASTIdentifier>(additional_column);
            result.expression_list_ast->children.insert(result.expression_list_ast->children.begin(), column_identifier);
        }

        for (const auto & additional_column : settings->ext_columns_back)
        {
            ASTPtr column_identifier = std::make_shared<ASTIdentifier>(additional_column);
            result.expression_list_ast->children.push_back(column_identifier);
        }
    }

    const auto & children = result.expression_list_ast->children;
    for (const auto & child : children)
        result.column_names.emplace_back(child->getColumnName());

    {
        auto expr = result.expression_list_ast->clone();
        auto syntax_result = TreeRewriter(context_).analyze(expr, columns_.getAllPhysical());
        /// In expression we also need to store source columns
        result.expression = ExpressionAnalyzer(expr, syntax_result, context_).getActions(false);
        /// In sample block we use just key columns
        result.sample_block = ExpressionAnalyzer(expr, syntax_result, context_).getActions(true)->getSampleBlock();
    }

    for (size_t i = 0; i < result.sample_block.columns(); ++i)
    {
        result.data_types.emplace_back(result.sample_block.getByPosition(i).type);
        if (!result.data_types.back()->isComparable())
            throw Exception(
                ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_KEY,
                "Column {} with type {} is not allowed in key expression, it's not comparable",
                backQuote(result.sample_block.getByPosition(i).name),
                result.data_types.back()->getName());
    }

    return result;
}

/// Parses description from string
KeyDescription KeyDescription::Builder::buildFromString(const String & str_, const ColumnsDescription & columns_, ContextPtr context_)
{
    KeyDescription result;
    if (str_.empty())
        return result;

    ParserExpression parser;
    ASTPtr ast = parseQuery(parser, "(" + str_ + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    FunctionNameNormalizer().visit(ast.get());

    return buildFromAST(ast, columns_, context_);
}

KeyDescription::KeyDescription(const KeyDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , sample_block(other.sample_block)
    , column_names(other.column_names)
    , data_types(other.data_types)
    , additional_settings(other.additional_settings)
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
    if (additional_settings.has_value() && !other.additional_settings.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong key assignment, losing additional_settings");

    additional_settings = other.additional_settings;

    return *this;
}


void KeyDescription::recalculateWithNewAST(const ASTPtr & new_ast, const ColumnsDescription & columns, ContextPtr context)
{
    *this = Builder(std::move(additional_settings)).buildFromAST(new_ast, columns, context);
}

void KeyDescription::recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr context)
{
    *this = Builder(std::move(additional_settings)).buildFromAST(definition_ast, new_columns, context);
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

}
