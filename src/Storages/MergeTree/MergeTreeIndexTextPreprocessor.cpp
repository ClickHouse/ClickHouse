#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn_fwd.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
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
/// later is correct.
void validatePreprocessorASTExpression(const ASTFunction * function, const String & identifier_name)
{
    chassert(!identifier_name.empty());
    chassert(function != nullptr);

    if (function->arguments == nullptr)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Preprocessor function has no arguments");

    for (const auto & argument : function->arguments->children)
    {
        /// In principle all literals are valid
        if (argument->as<ASTLiteral>())
            continue;

        /// We must have only the valid identifier_name as argument
        if (const ASTIdentifier * identifier = argument->as<ASTIdentifier>())
        {
            if (identifier_name != identifier->name())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Preprocessor function must only reference column: {}, also got: {}",
                    identifier_name, identifier->name());

            continue;
        }

        /// Check the arguments recursively (won't happen more than 2 or 3 times on average so it's fine).
        if (const ASTFunction * subfunction = argument->as<ASTFunction>())
        {
            validatePreprocessorASTExpression(subfunction, identifier_name);
            continue;
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Preprocessor function expects a literal or identifier as argument");
    }
}

DataTypePtr getInnerType(DataTypePtr type)
{
    DataTypePtr inner_type = type;

    if (isArray(type))
    {
        const DataTypeArray * array_type = typeid_cast<const DataTypeArray*>(type.get());
        inner_type = array_type->getNestedType();
    }
    else if (type->lowCardinality())
    {
        const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get());
        inner_type = low_cardinality_type->getDictionaryType();
    }

    if (isStringOrFixedString(inner_type))
        return inner_type;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Text index column type {} is not supported", type->getName());
}

}

MergeTreeIndexTextPreprocessor::MergeTreeIndexTextPreprocessor(const String & expression_str, const IndexDescription & index_description)
    : expression(MergeTreeIndexTextPreprocessor::parseExpression(index_description, expression_str))
    , inner_type(getInnerType(index_description.data_types.front()))
    , column_name(index_description.column_names.front())
{
}

std::pair<ColumnPtr,size_t> MergeTreeIndexTextPreprocessor::processColumn(const ColumnWithTypeAndName & column, size_t start_row, size_t n_rows) const
{
    chassert(column.name == column_name);

    ColumnPtr index_column = column.column;
    chassert(index_column->getDataType() == column.type->getTypeId());

    if (expression.getActions().empty())
        return {index_column, start_row};

    /// Only copy if needed
    if (start_row != 0 || n_rows != index_column->size())
        index_column = index_column->cut(start_row, n_rows);

    if (isArray(column.type))
    {
        const ColumnArray * column_array = assert_cast<const ColumnArray *>(index_column.get());
        const DataTypePtr nested_type = getInnerType(column.type);

        const ColumnPtr array_data = column_array->getDataPtr();
        const ColumnPtr array_offsets = column_array->getOffsetsPtr();

        ColumnWithTypeAndName array_data_column(array_data, nested_type, column_name);

        auto [processed_column, _] = processColumn(array_data_column, 0, array_data_column.column->size());

        ColumnPtr result_column = ColumnArray::create(processed_column, array_offsets);

        return {result_column, 0};
    }
    else
    {
        Block block({ColumnWithTypeAndName(index_column, column.type, column_name)});
        expression.execute(block, n_rows);
        return {block.safeGetByPosition(0).column, 0};
    }
}

String MergeTreeIndexTextPreprocessor::process(const String & input) const
{
    if (expression.getActions().empty())
        return input;

    Field input_field(input);
    ColumnWithTypeAndName input_entry(inner_type->createColumnConst(1, input_field), inner_type, column_name);

    Block input_block;
    input_block.insert(input_entry);

    size_t nrows = 1;
    expression.execute(input_block, nrows);

    return String{input_block.safeGetByPosition(0).column->getDataAt(0)};
}

ExpressionActions MergeTreeIndexTextPreprocessor::parseExpression(const IndexDescription & index_description, const String & expression)
{
    chassert(index_description.column_names.size() == 1);
    chassert(index_description.data_types.size() == 1);

    /// Empty expression still creates a preprocessor without actions.
    if (expression.empty())
        return ExpressionActions(ActionsDAG());

    /// This parser received the string stored from the expression's `column_name` or empty if no preprocessor set.
    /// `column_name` (from the DAG) should never be a blank string.
    /// But we add this tests in case someone decides to "manipulate" the expression before arriving here.
    if (expression.find_first_not_of(" \t\n\v\f\r") == std::string::npos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Preprocessor expression contains a blank non-empty string");

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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not parse preprocessor expression");

        /// Repeat expression validation here. after the string has been parsed into an AST.
        /// We already made this check during index construction, but "don't trust, verify"
        const ASTFunction * preprocessor_function = expression_ast->as<ASTFunction>();
        if (preprocessor_function == nullptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Preprocessor argument must be an expression");

        /// Now we know that the only valid identifier_name must be the text indexed column.
        String identifier_name = index_description.column_names.front();
        validatePreprocessorASTExpression(preprocessor_function, identifier_name);
    }

    /// Convert ASTPtr -> ActionsDAG
    /// We can do less checks here because in porevious scope we tested that the ASTPtr es an ASTFunction.
    const String name = expression_ast->getColumnName();
    const String alias = expression_ast->getAliasOrColumnName();

    DataTypePtr column_data_type = getInnerType(index_description.data_types.front());


    NamesAndTypesList source_columns({{index_description.column_names.front(), column_data_type}});

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
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must return only a single value");

    if (!isStringOrFixedString(outputs.front()->result_type))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression should return a String or a FixedString.");

    if (actions.hasNonDeterministic())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must not contain non-deterministic functions");

    /// FINALLY! Lets build the ExpressionActions.
    return ExpressionActions(std::move(actions));
}

}
