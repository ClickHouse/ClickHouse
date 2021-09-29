#include "SerializationUserDefinedType.h"
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

SerializationUserDefinedType::SerializationUserDefinedType(
    const SerializationPtr & nested_,
    const ASTPtr & nested_ast_,
    const ASTPtr & input_,
    const ASTPtr & output_,
    ContextPtr context_)
    : nested(nested_)
    , nested_ast(nested_ast_)
    , input(input_)
    , output(output_)
    , context(context_)
{
    string_serialization = std::make_shared<SerializationString>();
}

void SerializationUserDefinedType::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    nested->serializeBinary(field, ostr);
}

void SerializationUserDefinedType::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    nested->deserializeBinary(field, istr);
}

void SerializationUserDefinedType::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    nested->serializeBinary(column, row_num, ostr);
}

void SerializationUserDefinedType::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    nested->deserializeBinary(column, istr);
}

void SerializationUserDefinedType::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    string_serialization->serializeText(*convertToStringColumn(column), row_num, ostr, settings);
}

void SerializationUserDefinedType::deserializeText(IColumn & column, ReadBuffer & buffer, const FormatSettings & settings) const
{
    column = convertFromStringColumn([this, &buffer, &settings](MutableColumnPtr & string_column)
        {
            string_serialization->deserializeTextQuoted(*string_column, buffer, settings);
        })->assumeMutableRef();
}

void SerializationUserDefinedType::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    string_serialization->serializeTextJSON(*convertToStringColumn(column), row_num, ostr, settings);
}

void SerializationUserDefinedType::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    column = convertFromStringColumn([this, &istr, &settings](MutableColumnPtr & string_column)
        {
            string_serialization->deserializeTextJSON(*string_column, istr, settings);
        })->assumeMutableRef();
}

void SerializationUserDefinedType::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    string_serialization->serializeTextXML(*convertToStringColumn(column), row_num, ostr, settings);
}

void SerializationUserDefinedType::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    string_serialization->serializeTextCSV(*convertToStringColumn(column), row_num, ostr, settings);
}

void SerializationUserDefinedType::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    column = convertFromStringColumn([this, &istr, &settings](MutableColumnPtr & string_column)
        {
            string_serialization->deserializeTextCSV(*string_column, istr, settings);
        })->assumeMutableRef();
}

ColumnPtr SerializationUserDefinedType::convertToStringColumn(const IColumn & source_column) const
{
    auto column_ptr = source_column.getPtr();
    auto column_name = column_ptr->getFamilyName();
    auto data_type_ptr = DataTypeFactory::instance().get(column_name);
    ColumnsWithTypeAndName arguments;
    arguments.emplace_back(column_ptr, data_type_ptr, column_name);
    return executeFunction(output, arguments);
}

ColumnPtr SerializationUserDefinedType::convertFromStringColumn(std::function<void(MutableColumnPtr &)> string_deserializator) const
{
    auto string_type_ptr = DataTypeFactory::instance().get("String");
    MutableColumnPtr mutable_column_ptr = string_type_ptr->createColumn();
    string_deserializator(mutable_column_ptr);
    ColumnPtr column_ptr = std::move(mutable_column_ptr);
    ColumnsWithTypeAndName arguments;
    arguments.emplace_back(column_ptr, string_type_ptr, column_ptr->getFamilyName());
    auto return_type = DataTypeFactory::instance().get(nested_ast);
    return executeFunction(input, arguments);
}


void SerializationUserDefinedType::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    nested->enumerateStreams(callback, path);
}

void SerializationUserDefinedType::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(settings, state);
}

void SerializationUserDefinedType::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationUserDefinedType::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state);
}

void SerializationUserDefinedType::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
}

void SerializationUserDefinedType::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    nested->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
}

ColumnPtr SerializationUserDefinedType::executeFunction(ASTPtr function_core, const ColumnsWithTypeAndName & arguments) const
{
    const auto * lambda_args_tuple = function_core->as<ASTFunction>()->arguments->children.at(0)->as<ASTFunction>();
    const ASTs & lambda_arg_asts = lambda_args_tuple->arguments->children;

    NamesAndTypesList lambda_arguments;
    Block block;

    for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
    {
        auto opt_arg_name = tryGetIdentifierName(lambda_arg_asts[j]);
        if (!opt_arg_name)
            throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

        lambda_arguments.emplace_back(*opt_arg_name, arguments[j].type);
        auto column_ptr = arguments[j].column;
        if (!column_ptr)
            column_ptr = arguments[j].type->createColumnConstWithDefaultValue(1);
        block.insert({column_ptr, arguments[j].type, *opt_arg_name});
    }

    ASTPtr lambda_body = function_core->as<ASTFunction>()->children.at(0)->children.at(1);
    auto syntax_result = TreeRewriter(context).analyze(lambda_body, lambda_arguments);
    ExpressionAnalyzer analyzer(lambda_body, syntax_result, context);
    ExpressionActionsPtr actions = analyzer.getActions(false);

    actions->execute(block);
    return block.getColumns().back();
}

}
