#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>

#include <algorithm>

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

namespace
{
    /// The grid has at least millisecond precision, so fractional parameters are not truncated to whole seconds
    /// when the timestamps in the input columns have a coarser scale.
    constexpr UInt32 MIN_PARAMETERS_SCALE = 3;
}


void checkTimeseriesAggregateFunctionsEnabled(const std::string & name, const Settings * settings)
{
    if (settings && (*settings)[Setting::enable_time_series_aggregate_functions] == 0 && (*settings)[Setting::enable_time_series_table] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is in private preview and disabled by default. Enable it with setting enable_time_series_aggregate_functions",
            name);
}


void assertTimeseriesParametersCount(const std::string & name, const Array & parameters, size_t expected_parameter_count, std::string_view parameter_names)
{
    if (parameters.size() != expected_parameter_count)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires {} parameters: {}", name, expected_parameter_count, parameter_names);
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


UInt32 getTimeseriesParametersScale(const Array & parameters)
{
    UInt32 scale = MIN_PARAMETERS_SCALE;
    const size_t num_grid_parameters = std::min<size_t>(parameters.size(), 4);
    for (size_t i = 0; i < num_grid_parameters; ++i)
    {
        const auto & parameter = parameters[i];
        if (parameter.getType() == Field::Types::Decimal64)
            scale = std::max(scale, parameter.safeGet<DecimalField<Decimal64>>().getScale());
        else if (parameter.getType() == Field::Types::Decimal32)
            scale = std::max(scale, parameter.safeGet<DecimalField<Decimal32>>().getScale());
    }
    return scale;
}


std::pair<DataTypePtr, DataTypePtr> getTimeseriesTimestampAndValueTypes(const std::string & name, const DataTypes & argument_types, size_t num_extra_arguments)
{
    if (argument_types.size() != 1 + num_extra_arguments && argument_types.size() != 2 + num_extra_arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires {} or {} arguments: the samples as (timestamp, value) or as a single array of tuples{}",
            name, 1 + num_extra_arguments, 2 + num_extra_arguments,
            num_extra_arguments ? fmt::format(", followed by {} more argument(s)", num_extra_arguments) : "");

    DataTypes sample_types(argument_types.begin(), argument_types.end() - num_extra_arguments);

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
