#include <DataTypes/Serializations/SerializationUserDefined.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Columns/IColumn.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <DataTypes/IDataType.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TYPE_MISMATCH;
}

SerializationUserDefined::SerializationUserDefined(
    const SerializationPtr & nested_serialization_,
    DataTypePtr base_data_type_,
    const String & input_function_name_,
    const String & output_function_name_,
    ContextPtr context_)
    : nested_serialization(nested_serialization_)
    , base_data_type(base_data_type_)
    , input_function_name(input_function_name_)
    , output_function_name(output_function_name_)
    , context(context_)
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_DEBUG(log, "Constructor called. Input func: '{}', Output func: '{}', Base type: {}", 
              input_function_name, output_function_name, base_data_type ? base_data_type->getName() : "null");

    if (!nested_serialization)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SerializationUserDefined requires a nested serialization");
    if (!base_data_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SerializationUserDefined requires a base_data_type");
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SerializationUserDefined requires a Context");
}

ColumnPtr SerializationUserDefined::executeFunction(const String & function_name, const Field & field) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "executeFunction (Field) called for function '{}'", function_name);

    if (!base_data_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT function '{}' (Field input)", function_name);

    MutableColumnPtr temp_arg_col = base_data_type->createColumn();
    temp_arg_col->insert(field);

    auto & factory = FunctionFactory::instance();
    auto func_builder = factory.get(function_name, context);

    Block temp_block_for_func;
    temp_block_for_func.insert({std::move(temp_arg_col), base_data_type, "arg"});

    ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
    auto func = func_builder->build({argument_for_func});

    auto result_type_from_func = func->getResultType();
    ColumnPtr result_col = func->execute({argument_for_func}, result_type_from_func, 1, false);

    if (!result_type_from_func->equals(*base_data_type))
    {
        LOG_DEBUG(log, "Function '{}' for UDT (Field input) returned type {} which differs from base type {}. Attempting to CAST.",
                  function_name, result_type_from_func->getName(), base_data_type->getName());

        auto cast_func_builder = FunctionFactory::instance().get("CAST", context);
        Block cast_block;
        cast_block.insert({result_col, result_type_from_func, "value_to_cast"});
        cast_block.insert({ColumnConst::create(DataTypeString().createColumnConst(1, base_data_type->getName()), 1), std::make_shared<DataTypeString>(), "target_type_name"});

        ColumnsWithTypeAndName cast_arguments = {cast_block.getByPosition(0), cast_block.getByPosition(1)};
        auto cast_func = cast_func_builder->build(cast_arguments);
        
        ColumnPtr casted_result_col = cast_func->execute(cast_arguments, base_data_type, result_col->size(),false);

        result_col = casted_result_col;
    }

    if (result_col->size() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function '{}' (Field input, after potential cast) did not return a single value", function_name);
    }
    return result_col;
}

ColumnPtr SerializationUserDefined::executeFunction(const String & function_name, const IColumn & current_column, size_t row_num) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "executeFunction (IColumn, row_num) called for function '{}' on row {}", function_name, row_num);

    if (!base_data_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT function '{}' (IColumn row input)", function_name);

    MutableColumnPtr temp_arg_col = base_data_type->createColumn();
    temp_arg_col->insertFrom(current_column, row_num);

    auto & factory = FunctionFactory::instance();
    auto func_builder = factory.get(function_name, context);

    Block temp_block_for_func;
    temp_block_for_func.insert({std::move(temp_arg_col), base_data_type, "arg"});

    ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
    auto func = func_builder->build({argument_for_func});

    auto result_type_from_func = func->getResultType();
    ColumnPtr result_col = func->execute({argument_for_func}, result_type_from_func, 1, false);

    if (!result_type_from_func->equals(*base_data_type))
    {
        LOG_DEBUG(log, "Function '{}' for UDT (IColumn row input) returned type {} which differs from base type {}. Attempting to CAST.",
                  function_name, result_type_from_func->getName(), base_data_type->getName());

        auto cast_func_builder = FunctionFactory::instance().get("CAST", context);
        Block cast_block;
        cast_block.insert({result_col, result_type_from_func, "value_to_cast"});
        cast_block.insert({ColumnConst::create(DataTypeString().createColumnConst(1, base_data_type->getName()), 1), std::make_shared<DataTypeString>(), "target_type_name"});

        ColumnsWithTypeAndName cast_arguments = {cast_block.getByPosition(0), cast_block.getByPosition(1)};
        auto cast_func = cast_func_builder->build(cast_arguments);

        ColumnPtr casted_result_col = cast_func->execute(cast_arguments, base_data_type, result_col->size(), false);

        result_col = casted_result_col;
    }

    if (result_col->size() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function '{}' (IColumn row input, after potential cast) did not return a single value", function_name);
    }
    return result_col;
}

ColumnPtr SerializationUserDefined::executeFunction(const String & function_name, ColumnPtr & current_column_ptr) const
{
    Poco::Logger* log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "executeFunction (ColumnPtr) called for function '{}' on column of size {}", function_name, current_column_ptr ? current_column_ptr->size() : 0);

    if (!base_data_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT function '{}' (ColumnPtr input)", function_name);
    if (!current_column_ptr || current_column_ptr->empty())
    {
        LOG_TRACE(log, "Input column for function '{}' is null or empty, returning as is.", function_name);
        return current_column_ptr;
    }

    auto & factory = FunctionFactory::instance();
    auto func_builder = factory.get(function_name, context);

    Block temp_block_for_func;
    temp_block_for_func.insert({current_column_ptr, base_data_type, "arg"});

    ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
    auto func = func_builder->build({argument_for_func});

    auto result_type_from_func = func->getResultType();
    ColumnPtr result_col = func->execute({argument_for_func}, result_type_from_func, current_column_ptr->size(), false);
    
    if (!result_type_from_func->equals(*base_data_type))
    {
        LOG_DEBUG(log, "Function '{}' for UDT (ColumnPtr input) returned type {} which differs from base type {}. Attempting to CAST.",
                  function_name, result_type_from_func->getName(), base_data_type->getName());

        auto cast_func_builder = FunctionFactory::instance().get("CAST", context);
        Block cast_block;
        cast_block.insert({result_col, result_type_from_func, "value_to_cast"});
        size_t num_rows = result_col->size();
        cast_block.insert({ColumnConst::create(DataTypeString().createColumnConst(1, base_data_type->getName()), num_rows), std::make_shared<DataTypeString>(), "target_type_name"});

        ColumnsWithTypeAndName cast_arguments = {cast_block.getByPosition(0), cast_block.getByPosition(1)};
        auto cast_func = cast_func_builder->build(cast_arguments);
        
        ColumnPtr casted_result_col = cast_func->execute(cast_arguments, base_data_type, num_rows, false);

        result_col = casted_result_col;
    }

    if (result_col->size() != current_column_ptr->size())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Function '{}' for user-defined type (ColumnPtr input, after potential cast) changed column size from {} to {}. This is not supported.",
                        function_name, current_column_ptr->size(), result_col->size());
    }
    return result_col;
}

void SerializationUserDefined::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING serializeText for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "serializeText called. Output func: '{}'", output_function_name);

    if (!output_function_name.empty())
    {
        if (!base_data_type)
             throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT output function");

        MutableColumnPtr temp_base_col = base_data_type->createColumn();
        temp_base_col->insertFrom(column, row_num);

        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(output_function_name, context);
        
        Block temp_block_for_func;
        temp_block_for_func.insert({std::move(temp_base_col), base_data_type, "arg"});
        
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});

        auto result_type = func->getResultType();
        ColumnPtr result_col = func->execute({argument_for_func}, result_type, 1, false);

        if (!isStringOrFixedString(result_type) || result_col->size() != 1)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Output function '{}' for user-defined type did not return a single string value. Returned type: {}, size: {}",
                            output_function_name, result_type->getName(), result_col->size());
        }
        SerializationPtr string_serialization = result_type->getDefaultSerialization();
        string_serialization->serializeText(*result_col, 0, ostr, settings);
    }
    else
    {
        nested_serialization->serializeText(column, row_num, ostr, settings);
    }
}

void SerializationUserDefined::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "################################################################");
    LOG_ERROR(log, "ENTERING deserializeWholeText for UDT '{}'. Input func: '{}'", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE", input_function_name);
    LOG_ERROR(log, "################################################################");

    if (!input_function_name.empty())
    {
        if (!base_data_type || !nested_serialization)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type or nested serialization is not set for UDT input function");

        MutableColumnPtr temp_base_col_after_nested_deserialize = base_data_type->createColumn();
        nested_serialization->deserializeWholeText(*temp_base_col_after_nested_deserialize, istr, settings);

        if (temp_base_col_after_nested_deserialize->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Nested deserialization for UDT input function resulted in an empty column");

        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(input_function_name, context);

        Block temp_block_for_func;
        ColumnPtr arg_col_ptr = temp_base_col_after_nested_deserialize->getPtr();
        temp_block_for_func.insert({arg_col_ptr, base_data_type, "arg"});
        
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});

        auto result_type_from_func = func->getResultType();
        ColumnPtr result_col_from_sql_func = func->execute({argument_for_func}, result_type_from_func, arg_col_ptr->size(), false);

        ColumnPtr final_result_col_for_insert = result_col_from_sql_func;
        if (!result_type_from_func->equals(*base_data_type))
        {
            LOG_DEBUG(log, "Input function '{}' (deserializeWholeText) returned type {} which differs from base type {}. Attempting to CAST.",
                      input_function_name, result_type_from_func->getName(), base_data_type->getName());

            auto cast_func_builder = FunctionFactory::instance().get("CAST", context);
            Block cast_block;
            cast_block.insert({result_col_from_sql_func, result_type_from_func, "value_to_cast"});
            size_t num_rows = result_col_from_sql_func->size();
            cast_block.insert({ColumnConst::create(DataTypeString().createColumnConst(1, base_data_type->getName()), num_rows), std::make_shared<DataTypeString>(), "target_type_name"});

            ColumnsWithTypeAndName cast_arguments = {cast_block.getByPosition(0), cast_block.getByPosition(1)};
            auto cast_func_for_text = cast_func_builder->build(cast_arguments);
            
            ColumnPtr casted_column = cast_func_for_text->execute(cast_arguments, base_data_type, num_rows, false);
            final_result_col_for_insert = casted_column;
        }
        
        if (final_result_col_for_insert->size() != arg_col_ptr->size())
        {
             throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Input function '{}' for UDT (deserializeWholeText, after potential cast) changed column size from {} to {}. This is not supported.",
                            input_function_name, arg_col_ptr->size(), final_result_col_for_insert->size());
        }
        
        LOG_ERROR(log, "deserializeWholeText for UDT '{}': About to insert column of size {} into target column. First value (if any): {}", 
                  base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE", 
                  final_result_col_for_insert->size(), 
                  final_result_col_for_insert->empty() ? "EMPTY" : DB::toString((*final_result_col_for_insert)[0]));

        column.insertRangeFrom(*final_result_col_for_insert, 0, final_result_col_for_insert->size());
    }
    else
    {
        nested_serialization->deserializeWholeText(column, istr, settings);
    }
}

void SerializationUserDefined::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING serializeTextEscaped for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "serializeTextEscaped called. Output func: '{}'", output_function_name);

    if (!output_function_name.empty())
    {
        if (!base_data_type) throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set");
        MutableColumnPtr temp_base_col = base_data_type->createColumn();
        temp_base_col->insertFrom(column, row_num);
        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(output_function_name, context);
        Block temp_block_for_func;
        temp_block_for_func.insert({std::move(temp_base_col), base_data_type, "arg"});
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});
        auto result_type = func->getResultType();
        ColumnPtr result_col = func->execute({argument_for_func}, result_type, 1, false);
        if (!isStringOrFixedString(result_type) || result_col->size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Output function '{}' did not return a single string value", output_function_name);
        SerializationPtr string_serialization = result_type->getDefaultSerialization();
        string_serialization->serializeTextEscaped(*result_col, 0, ostr, settings);
    }
    else
        nested_serialization->serializeTextEscaped(column, row_num, ostr, settings);
}

void SerializationUserDefined::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING deserializeTextEscaped for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "deserializeTextEscaped called. Input func: '{}'", input_function_name);

    if (!input_function_name.empty())
    {
        if (!base_data_type || !nested_serialization)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type or nested serialization is not set for UDT input function");

        // 1. Deserialize input string into a temporary column of base_data_type using nested_serialization
        MutableColumnPtr temp_base_col_after_nested_deserialize = base_data_type->createColumn();
        nested_serialization->deserializeTextEscaped(*temp_base_col_after_nested_deserialize, istr, settings);

        if (temp_base_col_after_nested_deserialize->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Nested deserialization (escaped) for UDT input function resulted in an empty column");

        // 2. Get the Function
        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(input_function_name, context);

        Block temp_block_for_func;
        temp_block_for_func.insert({temp_base_col_after_nested_deserialize->getPtr(), base_data_type, "arg"});
        
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0); // Argument is now of base_data_type
        auto func = func_builder->build({argument_for_func});

        // 3. Execute function and check result type (should still be base_data_type)
        auto result_type_from_func = func->getResultType();
        if (!result_type_from_func->equals(*base_data_type))
        {
             throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Input function \'{}\' for user-defined type (acting on base type, escaped) returned type {} which does not match base type {}",
                            input_function_name, result_type_from_func->getName(), base_data_type->getName());
        }

        ColumnPtr result_col_from_func = func->execute({argument_for_func}, result_type_from_func, temp_base_col_after_nested_deserialize->size(), false);
        
        if (result_col_from_func->size() != temp_base_col_after_nested_deserialize->size())
        {
             throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Input function \'{}\' for user-defined type (escaped) changed column size from {} to {}. This is not supported for INPUT functions.",
                            input_function_name, temp_base_col_after_nested_deserialize->size(), result_col_from_func->size());
        }
        
        // 4. Insert the result from the function into the target column
        column.insertRangeFrom(*result_col_from_func, 0, result_col_from_func->size());
    }
    else
        nested_serialization->deserializeTextEscaped(column, istr, settings);
}

void SerializationUserDefined::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING serializeTextQuoted for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "serializeTextQuoted called. Output func: '{}'", output_function_name);

    if (!output_function_name.empty())
    {
        if (!base_data_type) throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set");
        MutableColumnPtr temp_base_col = base_data_type->createColumn();
        temp_base_col->insertFrom(column, row_num);
        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(output_function_name, context);
        Block temp_block_for_func;
        temp_block_for_func.insert({std::move(temp_base_col), base_data_type, "arg"});
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});
        auto result_type = func->getResultType();
        ColumnPtr result_col = func->execute({argument_for_func}, result_type, 1, false);
        if (!isStringOrFixedString(result_type) || result_col->size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Output function '{}' did not return a single string value", output_function_name);
        SerializationPtr string_serialization = result_type->getDefaultSerialization();
        string_serialization->serializeTextQuoted(*result_col, 0, ostr, settings);
    }
    else
        nested_serialization->serializeTextQuoted(column, row_num, ostr, settings);
}

void SerializationUserDefined::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING deserializeTextQuoted for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "deserializeTextQuoted called. Input func: '{}'", input_function_name);

    if (!input_function_name.empty())
    {
        auto string_type = std::make_shared<DataTypeString>();
        auto string_serialization = string_type->getDefaultSerialization();
        MutableColumnPtr temp_string_col_for_read = string_type->createColumn();
        string_serialization->deserializeTextQuoted(*temp_string_col_for_read, istr, settings);
        if (temp_string_col_for_read->empty())
             throw Exception(ErrorCodes::LOGICAL_ERROR, "deserializeTextQuoted for input function of UDT read an empty string value");

        Field temp_field = (*temp_string_col_for_read)[0];
        auto temp_string_col_arg = string_type->createColumn();
        temp_string_col_arg->insert(temp_field);
        
        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(input_function_name, context);
        Block temp_block_for_func;
        temp_block_for_func.insert({std::move(temp_string_col_arg), string_type, "arg"});
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});
        auto result_type = func->getResultType();
         if (!base_data_type) throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set");
        if (!result_type->equals(*base_data_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input function '{}' returned type {} which does not match base type {}", input_function_name, result_type->getName(), base_data_type->getName());
        ColumnPtr result_col = func->execute({argument_for_func}, result_type, 1, false);
        if (result_col->size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input function '{}' did not return a single value", input_function_name);
        column.insertFrom(*result_col, 0);
    }
    else
        nested_serialization->deserializeTextQuoted(column, istr, settings);
}

void SerializationUserDefined::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING serializeTextJSON for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "serializeTextJSON called. Output func: '{}'", output_function_name);

    if (!output_function_name.empty())
    {
        if (!base_data_type) throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set");
        MutableColumnPtr temp_base_col = base_data_type->createColumn();
        temp_base_col->insertFrom(column, row_num);
        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(output_function_name, context);
        Block temp_block_for_func;
        temp_block_for_func.insert({std::move(temp_base_col), base_data_type, "arg"});
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});
        auto result_type = func->getResultType();
        ColumnPtr result_col = func->execute({argument_for_func}, result_type, 1, false);
        if (result_col->size() != 1)
             throw Exception(ErrorCodes::LOGICAL_ERROR, "Output function '{}' did not return a single value for JSON", output_function_name);
        SerializationPtr result_serialization = result_type->getDefaultSerialization();
        result_serialization->serializeTextJSON(*result_col, 0, ostr, settings);
    }
    else
        nested_serialization->serializeTextJSON(column, row_num, ostr, settings);
}

void SerializationUserDefined::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING deserializeTextJSON for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "deserializeTextJSON called. Input func: '{}'", input_function_name);

    if (!input_function_name.empty())
    {
        auto string_type = std::make_shared<DataTypeString>();
        auto string_serialization = string_type->getDefaultSerialization();
        MutableColumnPtr temp_string_col_for_read = string_type->createColumn();
        string_serialization->deserializeTextJSON(*temp_string_col_for_read, istr, settings);
         if (temp_string_col_for_read->empty())
             throw Exception(ErrorCodes::LOGICAL_ERROR, "deserializeTextJSON for input function of UDT read an empty string value");
        
        Field temp_field = (*temp_string_col_for_read)[0];
        auto temp_string_col_arg = string_type->createColumn();
        temp_string_col_arg->insert(temp_field);

        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(input_function_name, context);
        Block temp_block_for_func;
        temp_block_for_func.insert({std::move(temp_string_col_arg), string_type, "arg"});
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});
        auto result_type = func->getResultType();
        if (!base_data_type) throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set");
        if (!result_type->equals(*base_data_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input function '{}' returned type {} which does not match base type {}", input_function_name, result_type->getName(), base_data_type->getName());
        ColumnPtr result_col = func->execute({argument_for_func}, result_type, 1, false);
        if (result_col->size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input function '{}' did not return a single value", input_function_name);
        column.insertFrom(*result_col, 0);
    }
    else
        nested_serialization->deserializeTextJSON(column, istr, settings);
}

void SerializationUserDefined::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING serializeTextCSV for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "serializeTextCSV called. Output func: '{}'", output_function_name);

    if (!output_function_name.empty())
    {
        if (!base_data_type) throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set");
        MutableColumnPtr temp_base_col = base_data_type->createColumn();
        temp_base_col->insertFrom(column, row_num);
        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(output_function_name, context);
        Block temp_block_for_func;
        temp_block_for_func.insert({std::move(temp_base_col), base_data_type, "arg"});
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});
        auto result_type = func->getResultType();
        ColumnPtr result_col = func->execute({argument_for_func}, result_type, 1, false);
        if (!isStringOrFixedString(result_type) || result_col->size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Output function '{}' did not return a single string value for CSV", output_function_name);
        SerializationPtr string_serialization = result_type->getDefaultSerialization();
        string_serialization->serializeTextCSV(*result_col, 0, ostr, settings);
    }
    else
        nested_serialization->serializeTextCSV(column, row_num, ostr, settings);
}

void SerializationUserDefined::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_ERROR(log, "**************** ENTERING deserializeTextCSV for {} ****************", base_data_type ? base_data_type->getName() : "UNKNOWN_BASE_TYPE");
    LOG_TRACE(log, "deserializeTextCSV called. Input func: '{}'", input_function_name);

    if (!input_function_name.empty())
    {
        auto string_type = std::make_shared<DataTypeString>();
        auto string_serialization = string_type->getDefaultSerialization();
        MutableColumnPtr temp_string_col_for_read = string_type->createColumn();
        string_serialization->deserializeTextCSV(*temp_string_col_for_read, istr, settings);
        if (temp_string_col_for_read->empty())
             throw Exception(ErrorCodes::LOGICAL_ERROR, "deserializeTextCSV for input function of UDT read an empty string value");
        
        Field temp_field = (*temp_string_col_for_read)[0];
        auto temp_string_col_arg = string_type->createColumn();
        temp_string_col_arg->insert(temp_field);

        auto & factory = FunctionFactory::instance();
        auto func_builder = factory.get(input_function_name, context);
        Block temp_block_for_func;
        temp_block_for_func.insert({std::move(temp_string_col_arg), string_type, "arg"});
        ColumnWithTypeAndName argument_for_func = temp_block_for_func.getByPosition(0);
        auto func = func_builder->build({argument_for_func});
        auto result_type = func->getResultType();
        if (!base_data_type) throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set");
        if (!result_type->equals(*base_data_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input function '{}' returned type {} which does not match base type {}", input_function_name, result_type->getName(), base_data_type->getName());
        ColumnPtr result_col = func->execute({argument_for_func}, result_type, 1, false);
        if (result_col->size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input function '{}' did not return a single value", input_function_name);
        column.insertFrom(*result_col, 0);
    }
    else
        nested_serialization->deserializeTextCSV(column, istr, settings);
}

void SerializationUserDefined::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "serializeBinary (Field) called. Output func: '{}'", output_function_name);

    if (!output_function_name.empty())
    {
        if (!base_data_type) 
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT output function '{}' (Field input)", output_function_name);
        
        ColumnPtr result_col_from_func = executeFunction(output_function_name, field);
        Field field_to_serialize = (*result_col_from_func)[0];
        nested_serialization->serializeBinary(field_to_serialize, ostr, settings);
    }
    else
    {
        nested_serialization->serializeBinary(field, ostr, settings);
    }
}

void SerializationUserDefined::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "deserializeBinary (Field) called. Input func: '{}'", input_function_name);

    if (!input_function_name.empty())
    {
        if (!base_data_type) 
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT input function '{}' (Field input)", input_function_name);

        Field temp_field_after_nested_deserialize;
        nested_serialization->deserializeBinary(temp_field_after_nested_deserialize, istr, settings);
        
        ColumnPtr result_col_from_func = executeFunction(input_function_name, temp_field_after_nested_deserialize);
        field = (*result_col_from_func)[0];
    }
    else
    {
        nested_serialization->deserializeBinary(field, istr, settings);
    }
}

void SerializationUserDefined::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "serializeBinary (IColumn) called for row {}. Output func: '{}'", row_num, output_function_name);

    if (!output_function_name.empty())
    {
        if (!base_data_type) 
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT output function '{}' (IColumn input)", output_function_name);

        ColumnPtr result_col_from_func = executeFunction(output_function_name, column, row_num);
        nested_serialization->serializeBinary(*result_col_from_func, 0, ostr, settings);
    }
    else
    {
        nested_serialization->serializeBinary(column, row_num, ostr, settings);
    }
}

void SerializationUserDefined::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "deserializeBinary (IColumn) called. Input func: '{}'", input_function_name);

    if (!input_function_name.empty())
    {
        if (!base_data_type) 
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT input function '{}' (IColumn input)", input_function_name);

        MutableColumnPtr temp_col_after_nested_deserialize = base_data_type->createColumn();
        nested_serialization->deserializeBinary(*temp_col_after_nested_deserialize, istr, settings);

        if (temp_col_after_nested_deserialize->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Nested deserialization for UDT input function '{}' (IColumn input) resulted in an empty column", input_function_name);
        
        // temp_col_after_nested_deserialize has one row here
        ColumnPtr result_col_from_func = executeFunction(input_function_name, *temp_col_after_nested_deserialize, 0);
        column.insertFrom(*result_col_from_func, 0);
    }
    else
    {
        nested_serialization->deserializeBinary(column, istr, settings);
    }
}

void SerializationUserDefined::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "serializeBinaryBulk called for offset {} limit {}. Output func: '{}'", offset, limit, output_function_name);

    if (!output_function_name.empty())
    {
        if (!base_data_type) 
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT output function '{}' (serializeBinaryBulk)", output_function_name);

        ColumnPtr source_column_ptr_for_func;
        size_t num_rows_to_process;

        if (offset >= column.size()) // Offset is beyond or at the end of the column
        {
            source_column_ptr_for_func = column.cloneEmpty();
            num_rows_to_process = 0;
        }
        else
        {
            size_t effective_limit = limit;
            // If limit is 0, or if limit goes beyond the actual number of available rows from offset
            if (limit == 0 || offset + limit > column.size())
                effective_limit = column.size() - offset;
            
            if (effective_limit == 0) // No rows to process with the given offset and limit
            {
                 source_column_ptr_for_func = column.cloneEmpty();
                 num_rows_to_process = 0;
            }
            // If offset is 0 and effective_limit covers the whole column, no cut is needed.
            else if (offset == 0 && effective_limit == column.size())
            {
                source_column_ptr_for_func = column.getPtr(); // Use the original column directly
                num_rows_to_process = column.size();
            }
            else // A subsegment of the column needs to be processed
            {
                source_column_ptr_for_func = column.cut(offset, effective_limit);
                num_rows_to_process = source_column_ptr_for_func->size();
            }
        }
        
        if (num_rows_to_process == 0)
        {
            LOG_TRACE(log, "serializeBinaryBulk: source column is effectively empty for output_function '{}', serializing empty.", output_function_name);
            auto empty_col_for_nested = column.cloneEmpty(); // Ensure correct type for nested_serialization
            nested_serialization->serializeBinaryBulk(*empty_col_for_nested, ostr, 0, 0); 
            return;
        }

        // source_column_ptr_for_func is ColumnPtr, executeFunction expects ColumnPtr&
        // Create a local ColumnPtr that can be passed by reference if needed by the helper, 
        // though the helper itself should handle constness appropriately if it doesn't modify the passed pointer itself.
        ColumnPtr arg_col_for_func = source_column_ptr_for_func; 
        ColumnPtr result_column_ptr = executeFunction(output_function_name, arg_col_for_func);
        nested_serialization->serializeBinaryBulk(*result_column_ptr, ostr, 0, result_column_ptr->size());
    }
    else
    {
        nested_serialization->serializeBinaryBulk(column, ostr, offset, limit);
    }
}

void SerializationUserDefined::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "deserializeBinaryBulk called for rows_offset {} limit {}. Input func: '{}'", rows_offset, limit, input_function_name);

    if (!input_function_name.empty())
    {
        if (!base_data_type) 
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Base data type is not set for UDT input function '{}' (deserializeBinaryBulk)", input_function_name);

        MutableColumnPtr temp_column_for_nested_deserialize = column.cloneEmpty(); 
        
        nested_serialization->deserializeBinaryBulk(*temp_column_for_nested_deserialize, istr, rows_offset, limit, avg_value_size_hint);

        if (temp_column_for_nested_deserialize->empty())
        {
            LOG_TRACE(log, "deserializeBinaryBulk: column is empty after nested deserialization for input_function '{}'.", input_function_name);
            MutableColumnPtr mutable_original_column = column.assumeMutable();
            if (mutable_original_column->size() > 0) // Clear only if it had data
                mutable_original_column->popBack(mutable_original_column->size());
            return;
        }

        ColumnPtr column_ptr_for_func = temp_column_for_nested_deserialize->getPtr();
        ColumnPtr result_column_ptr = executeFunction(input_function_name, column_ptr_for_func);

        MutableColumnPtr mutable_original_column = column.assumeMutable();
        if (mutable_original_column->size() > 0)
            mutable_original_column->popBack(mutable_original_column->size()); 
        
        if (result_column_ptr && result_column_ptr->size() > 0)
            mutable_original_column->insertRangeFrom(*result_column_ptr, 0, result_column_ptr->size());
    }
    else
    {
        nested_serialization->deserializeBinaryBulk(column, istr, rows_offset, limit, avg_value_size_hint);
    }
}

void SerializationUserDefined::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "enumerateStreams called. UDT INPUT/OUTPUT functions are NOT applied here. Delegating to nested serialization.");
    nested_serialization->enumerateStreams(settings, callback, data);
}

void SerializationUserDefined::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "serializeBinaryBulkStatePrefix called. UDT OUTPUT function is NOT applied here. Delegating to nested serialization.");
    nested_serialization->serializeBinaryBulkStatePrefix(column, settings, state);
}

void SerializationUserDefined::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "serializeBinaryBulkStateSuffix called. UDT OUTPUT function is NOT applied here. Delegating to nested serialization.");
    nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationUserDefined::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "deserializeBinaryBulkStatePrefix called. UDT INPUT function is NOT applied here. Delegating to nested serialization.");
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state, cache);
}

void SerializationUserDefined::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "serializeBinaryBulkWithMultipleStreams called. UDT OUTPUT function is NOT applied here. Delegating to nested serialization.");
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
}

void SerializationUserDefined::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    Poco::Logger * log = &Poco::Logger::get("SerializationUserDefined");
    LOG_TRACE(log, "deserializeBinaryBulkWithMultipleStreams called. UDT INPUT function is NOT applied here. Delegating to nested serialization.");
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, state, cache);
}

}
