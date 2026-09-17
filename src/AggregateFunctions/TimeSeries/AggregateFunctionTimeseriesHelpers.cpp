#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
    extern const SettingsBool enable_time_series_aggregate_functions;
    extern const SettingsBool enable_time_series_table;
}


/// Extracts a timestamp parameter value and converts it to decimal with the target scale (scale of the timestamp column)
DateTime64 extractTimeseriesTimestampParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale)
{
    try
    {
        return parseTimeSeriesTimestamp(parameter_field, target_scale);
    }
    catch (Exception & e)
    {
        e.addMessage("While parsing {} parameter of aggregate function {}", parameter_name, function_name);
        throw;
    }
}

/// Extracts a duration parameter value and converts it to decimal with the target scale (scale of the timestamp column)
Decimal64 extractTimeseriesDurationParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale)
{
    try
    {
        return parseTimeSeriesDuration(parameter_field, target_scale);
    }
    catch (Exception & e)
    {
        e.addMessage("While parsing {} parameter of aggregate function {}", parameter_name, function_name);
        throw;
    }
}

UInt64 extractTimeseriesIntParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field)
{
    if (parameter_field.getType() == Field::Types::Decimal64)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal64>>();
        auto scale_multiplier = value.getScaleMultiplier();
        auto raw_value = value.getValue();
        if (scale_multiplier > 1 && raw_value % scale_multiplier != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot convert Decimal64 {} parameter to integer for aggregate function {}", parameter_name, function_name);
        return raw_value / scale_multiplier;
    }
    else if (parameter_field.getType() == Field::Types::Decimal32)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal32>>();
        auto scale_multiplier = value.getScaleMultiplier();
        auto raw_value = value.getValue();
        if (scale_multiplier > 1 && raw_value % scale_multiplier != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot convert Decimal32 {} parameter to integer for aggregate function {}", parameter_name, function_name);
        return raw_value / scale_multiplier;
    }
    else if (UInt64 int_value = 0; parameter_field.tryGet(int_value))
    {
        return int_value;
    }
    else if (String string_value; parameter_field.tryGet(string_value))
    {
        UInt64 value{};
        ReadBufferFromString buf(string_value);
        if (tryReadIntText(value, buf))
            return value;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot parse {} parameter for aggregate function {}", parameter_name, function_name);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of {} parameter for aggregate function {}",
            parameter_field.getTypeName(), parameter_name, function_name);
    }
}

Float64 extractTimeseriesFloatParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field)
{
    if (parameter_field.getType() == Field::Types::Decimal64)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal64>>();
        return static_cast<Float64>(value.getValue()) / static_cast<Float64>(value.getScaleMultiplier());
    }
    else if (parameter_field.getType() == Field::Types::Decimal32)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal32>>();
        return static_cast<Float64>(value.getValue()) / static_cast<Float64>(value.getScaleMultiplier());
    }
    else if (Float64 float_value = 0; parameter_field.tryGet(float_value))
    {
        return float_value;
    }
    else if (Int64 int_value = 0; parameter_field.tryGet(int_value))
    {
        return static_cast<Float64>(int_value);
    }
    else if (UInt64 uint_value = 0; parameter_field.tryGet(uint_value))
    {
        return static_cast<Float64>(uint_value);
    }
    else if (String string_value; parameter_field.tryGet(string_value))
    {
        Float64 value{};
        ReadBufferFromString buf(string_value);
        if (tryReadFloatTextPrecise(value, buf))
            return value;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot parse {} parameter for aggregate function {}", parameter_name, function_name);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of {} parameter for aggregate function {}",
            parameter_field.getTypeName(), parameter_name, function_name);
    }
}

/// Validates that the aggregate function got exactly the expected number of parameters.
void assertTimeseriesParametersCount(const std::string & name, const Array & parameters, size_t required, std::string_view parameter_names)
{
    if (parameters.size() != required)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires {} parameters: {}", name, required, parameter_names);
}

void checkTimeseriesAggregateFunctionsEnabled(const std::string & name, const Settings * settings)
{
    if (settings && (*settings)[Setting::enable_time_series_aggregate_functions] == 0 && (*settings)[Setting::enable_time_series_table] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is in private preview and disabled by default. Enable it with setting enable_time_series_aggregate_functions",
            name);
}

std::pair<DataTypePtr, DataTypePtr> getTimeseriesTimestampAndValueTypes(const std::string & name, const DataTypes & argument_types, bool has_grid_argument)
{
    DataTypes sample_types = argument_types;
    if (has_grid_argument)
    {
        if (argument_types.size() != 2 && argument_types.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function {} requires 2 or 3 arguments: the samples as (timestamp, value) or as a single array of tuples, followed by one more argument",
                name);

        const auto & grid_argument_type = argument_types.back();
        const auto * array_type = typeid_cast<const DataTypeArray *>(grid_argument_type.get());
        if (!isNativeNumber(array_type ? array_type->getNestedType() : grid_argument_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the last argument for aggregate function {}, expected a number or an array of numbers",
                grid_argument_type->getName(), name);

        sample_types.pop_back();
    }

    if (sample_types.size() == 1)
    {
        /// The single argument form: samples are passed as Array(Tuple(timestamp, value)).
        const auto * array_type = typeid_cast<const DataTypeArray *>(sample_types[0].get());
        const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
        if (!tuple_type || tuple_type->getElements().size() != 2)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function {}, expected Array(Tuple(timestamp, value))",
                sample_types[0]->getName(), name);

        return {tuple_type->getElements()[0], tuple_type->getElements()[1]};
    }

    if (sample_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires the samples as two arguments (timestamp, value) or as a single array of tuples", name);

    if ((sample_types[0]->getTypeId() == TypeIndex::Array) != (sample_types[1]->getTypeId() == TypeIndex::Array))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal combination of argument type {} and {} for aggregate function {}, expected both arguments to be arrays or not arrays",
            sample_types[0]->getName(), sample_types[1]->getName(), name);

    if (sample_types[1]->getTypeId() == TypeIndex::Array)
    {
        const auto & timestamp_type = typeid_cast<const DataTypeArray *>(sample_types[0].get())->getNestedType();
        const auto & value_type = typeid_cast<const DataTypeArray *>(sample_types[1].get())->getNestedType();
        return {timestamp_type, value_type};
    }

    return {sample_types[0], sample_types[1]};
}

}
