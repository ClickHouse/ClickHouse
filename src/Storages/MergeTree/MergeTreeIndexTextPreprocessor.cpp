#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsMatcher.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Storages/IndicesDescription.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
}


namespace
{
/// Early preprocessor argument validation.
/// Maybe we could omit this validation and use only the validate function. But here we do it early and simpler to ensure that what we parse
/// latter is correct
void validatePreprocessorASTExpression(const ASTFunction * function, const String & identifier_name)
{
    chassert(!identifier_name.empty());
    chassert(function != nullptr);
    if (function->arguments == nullptr)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Text index preprocessor argument functions expects a function with some arguments");

    for (const auto & argument : function->arguments->children)
    {
        /// In principle all literals are valid
        if (argument->as<ASTLiteral>())
            continue;

        /// We must have only the valid identifier_name as argument
        if (const ASTIdentifier * identifier = argument->as<ASTIdentifier>())
        {
            if (identifier_name != identifier->name())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index preprocessor function should receive only the column: {} in the arguments; but there is also: {}",
                    identifier_name, identifier->name());

            continue;
        }

        /// If there is a nested (sub) function, then we recursively check also it's arguments.
        /// I like to avoid recursive calls, but I won't expect more than 2 or 3 functions nesting levels here, so let's keep it simple.
        if (const ASTFunction * subfunction = argument->as<ASTFunction>())
        {
            validatePreprocessorASTExpression(subfunction, identifier_name);
            continue;
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index preprocessor function expect a literal or identifier as argument");
    }
}


bool isValidTextIndexType(const DataTypePtr type)
{
    if (isStringOrFixedString(type))
        return true;

    const DataTypeArray * array_type = typeid_cast<const DataTypeArray*>(type.get());
    if (array_type != nullptr && isStringOrFixedString(array_type->getNestedType()))
        return true;

    return false;
}

}

MergeTreeIndexTextPreprocessor::MergeTreeIndexTextPreprocessor(const String & expression_str, const IndexDescription & index_description)
    : expression(MergeTreeIndexTextPreprocessor::parseExpression(index_description, expression_str))
    , column_type(index_description.data_types.front())
    , column_name(index_description.column_names.front())
{
}

std::pair<ColumnPtr,size_t> MergeTreeIndexTextPreprocessor::processColumn(const ColumnWithTypeAndName & index_column_with_type_and_name, size_t start_row, size_t n_rows) const
{
    ColumnPtr index_column = index_column_with_type_and_name.column;

    if (expression.getActions().empty())
        return {index_column, start_row};

    if (start_row != 0 || n_rows != index_column->size())
        index_column = index_column->cut(start_row, n_rows);

    Block block({ColumnWithTypeAndName(index_column, index_column_with_type_and_name.type, index_column_with_type_and_name.name)});

    expression.execute(block, n_rows);

    return {block.safeGetByPosition(0).column, 0};
}

String MergeTreeIndexTextPreprocessor::process(const String &input) const
{
    if (expression.getActions().empty())
        return input;

    Field field(input);
    ColumnWithTypeAndName entry(column_type->createColumnConst(1, field), column_type, column_name);

    Block block;
    block.insert(entry);

    size_t nrows = 1;
    expression.execute(block, nrows);

    return block.safeGetByPosition(0).column->getDataAt(0).toString();
}

ExpressionActions MergeTreeIndexTextPreprocessor::parseExpression(const IndexDescription & index_description, const String & expression)
{
    chassert(index_description.column_names.size() == 1);
    chassert(index_description.data_types.size() == 1);

    /// Empty expression still creates a preprocessor with empty actions.
    if (expression.empty())
        return ExpressionActions(ActionsDAG());

    /// This parser received the string stored from the expression's `column_name` or empty if no preprocessor set.
    /// `column_name` (from the DAG) should never be a blank string.
    /// But we add this tests in case someone decides to "manipulate" the expression before arriving here.
    if (expression.find_first_not_of(" \t\n\v\f\r") == std::string::npos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index preprocessor parser received a blank non empty string.");

    const char * expression_begin = &*expression.begin();
    const char * expression_end = &*expression.end();

    /// These are expression tokens, do not confuse with index tokens
    Tokens tokens(expression_begin, expression_end);
    IParser::Pos token_iterator(tokens, 1000, 1000000);

    Expected expected;
    ASTPtr expression_ast;

    { /// Parse and verify the expression: String -> ASTPtr
        ParserExpression parser_expression;
        if (!parser_expression.parse(token_iterator, expression_ast, expected))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error parsing preprocessor expression");

        /// Repeat expression validation here. after the string has been parsed into an AST.
        /// We already made this check during index construction, but "don't trust, verify"
        const ASTFunction * preprocessor_function = expression_ast->as<ASTFunction>();
        if (preprocessor_function == nullptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index preprocessor argument must be an expression");

        // Now we know that the only valid identifier_name must be the text indexed column.
        String identifier_name = index_description.column_names.front();
        validatePreprocessorASTExpression(preprocessor_function, identifier_name);
    }

    /// Convert ASTPtr -> ActionsDAG
    /// We can do less checks here because in porevious scope we tested that the ASTPtr es an ASTFunction.
    const String name = expression_ast->getColumnName();
    const String alias = expression_ast->getAliasOrColumnName();

    NamesAndTypesList source_columns({{index_description.column_names.front(), index_description.data_types.front()}});

    NamesAndTypesList aggregation_keys;
    ColumnNumbersList aggregation_keys_indexes_list;

    /// ActionsVisitor::Data needs a context argument to select the UDF factory and to pass to IFunction constructors
    /// The preprocessor expression is very simple and in most of the cases won't use the context at all. But it cannot be nullptr
    ///
    /// Strictly speaking we must use the local context instead of getGlobalContextInstance; but local context needs to be passed throw the
    /// indices constructor call stack. Those changes will affect all the Indices' Constructors (and Visitors) interfaces and imply modifying
    /// too many files with apparently no benefits.
    /// I let this comment her to remember in case we face some issues in the future for using the global context.
    ///
    /// Construct a visitor to parse the AST to build a DAG
    ActionsVisitor::Data visitor_data(
        Context::getGlobalContextInstance(),
        SizeLimits() /* set_size_limit */,
        0 /* subquery_depth */,
        source_columns,
        ActionsDAG(source_columns),
        {} /* prepared_sets */,
        false /* no_makeset_for_subqueries */,
        false /* no_makeset */,
        false /* only_consts */,
        AggregationKeysInfo(aggregation_keys, aggregation_keys_indexes_list, GroupByKind::NONE)
    );
    ActionsVisitor(visitor_data).visit(expression_ast);

    ActionsDAG actions = visitor_data.getActions();
    actions.project(NamesWithAliases({{name, alias}}));

    /// With the dag we can create an ExpressionActions. But before that is better to perform some validations.

    /// Lets check expression outputs
    ActionsDAG::NodeRawConstPtrs & outputs = actions.getOutputs();
    if (outputs.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must return only one argument");

    if (!isValidTextIndexType(outputs.front()->result_type))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression should return a String, FixedString or an array of them.");

    if (actions.hasNonDeterministic())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must contain only deterministic members.");

    /// FINALLY! Lets build the ExpressionActions.
    return ExpressionActions(std::move(actions));
}

}
