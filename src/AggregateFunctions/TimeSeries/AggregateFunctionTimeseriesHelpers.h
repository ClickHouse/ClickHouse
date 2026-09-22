#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/IDataType.h>

#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Helpers shared by the factories of the timeSeries*ToGrid aggregate functions.

/// Extracts a timestamp parameter value and converts it to decimal with the target scale (scale of the timestamp column)
DateTime64 extractTimeseriesTimestampParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale);

/// Extracts a duration parameter value and converts it to decimal with the target scale (scale of the timestamp column)
Decimal64 extractTimeseriesDurationParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale);

UInt64 extractTimeseriesIntParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);

Float64 extractTimeseriesFloatParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);

/// Validates that the aggregate function got exactly the expected number of parameters.
void assertTimeseriesParametersCount(const std::string & name, const Array & parameters, size_t required, std::string_view parameter_names);

/// Throws if the time series aggregate functions are disabled by the settings.
void checkTimeseriesAggregateFunctionsEnabled(const std::string & name, const Settings * settings);

/// Validates the argument types of a timeSeries*ToGrid function and returns the timestamp and value types of the samples.
/// `has_grid_argument` is set for functions taking one more argument after the samples, a number or an array of numbers
/// (see `AggregateFunctionTimeseriesBase::num_extra_arguments`).
std::pair<DataTypePtr, DataTypePtr> getTimeseriesTimestampAndValueTypes(const std::string & name, const DataTypes & argument_types, bool has_grid_argument);


/// With the timestamp, interval and value types resolved, parse the grid parameters and build the function via
/// the factory. The factory receives the parsed values and is the only place that knows the concrete function.
template <
    typename TimestampType,
    typename IntervalType,
    typename ValueType,
    typename MakeFunction
>
AggregateFunctionPtr createAggregateFunctionTimeseriesWithTypes(const std::string & name, const Array & parameters, UInt32 target_scale, MakeFunction && make_function)
{
    if constexpr (std::is_same_v<TimestampType, DateTime64>)
    {
        /// Convert start, end, step and staleness parameters to the scale of the timestamp column
        DateTime64 start_timestamp = extractTimeseriesTimestampParameter(name, "start", parameters[0], target_scale);
        DateTime64 end_timestamp = extractTimeseriesTimestampParameter(name, "end", parameters[1], target_scale);
        DateTime64 step = extractTimeseriesDurationParameter(name, "step", parameters[2], target_scale);
        DateTime64 window = extractTimeseriesDurationParameter(name, "window", parameters[3], target_scale);
        return make_function.template operator()<TimestampType, IntervalType, ValueType>(start_timestamp, end_timestamp, step, window, target_scale);
    }
    else
    {
        UInt64 start_timestamp = extractTimeseriesIntParameter(name, "start", parameters[0]);
        UInt64 end_timestamp = extractTimeseriesIntParameter(name, "end", parameters[1]);
        Int64 step = extractTimeseriesIntParameter(name, "step", parameters[2]);
        Int64 window = extractTimeseriesIntParameter(name, "window", parameters[3]);
        return make_function.template operator()<TimestampType, IntervalType, ValueType>(
            static_cast<TimestampType>(start_timestamp), static_cast<TimestampType>(end_timestamp),
            static_cast<IntervalType>(step), static_cast<IntervalType>(window), target_scale);
    }
}

/// Resolves the timestamp and interval types from the timestamp type, then delegates to createAggregateFunctionTimeseriesWithTypes.
template <
    typename ValueType,
    typename MakeFunction
>
AggregateFunctionPtr createAggregateFunctionTimeseriesWithValueType(const std::string & name, const Array & parameters, const DataTypePtr & timestamp_type, MakeFunction && make_function)
{
    if (isDateTime64(timestamp_type))
    {
        auto timestamp_decimal = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type);
        return createAggregateFunctionTimeseriesWithTypes<DateTime64, Int64, ValueType>(name, parameters, timestamp_decimal->getScale(), make_function);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        return createAggregateFunctionTimeseriesWithTypes<UInt32, Int32, ValueType>(name, parameters, 0, make_function);
    }

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of the timestamps for aggregate function {}",
                    timestamp_type->getName(), name);
}

/// Resolves the value type, then delegates to createAggregateFunctionTimeseriesWithValueType.
template <typename MakeFunction>
AggregateFunctionPtr createAggregateFunctionTimeseriesWithTimestampAndValueTypes(const std::string & name, const Array & parameters,
    const DataTypePtr & timestamp_type, const DataTypePtr & value_type, MakeFunction && make_function)
{
    if (value_type->getTypeId() == TypeIndex::Float64)
        return createAggregateFunctionTimeseriesWithValueType<Float64>(name, parameters, timestamp_type, make_function);
    if (value_type->getTypeId() == TypeIndex::Float32)
        return createAggregateFunctionTimeseriesWithValueType<Float32>(name, parameters, timestamp_type, make_function);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of the values for aggregate function {}", value_type->getName(), name);
}

/// Entry point shared by every timeSeries*ToGrid function: validates the arguments, resolves the value type, and
/// builds the function through the given factory (a generic lambda templated on timestamp, interval and value types).
/// `has_grid_argument` is set for functions taking one more argument after the samples, a number or an array of numbers
/// (see `AggregateFunctionTimeseriesBase::num_extra_arguments`).
template <typename MakeFunction>
AggregateFunctionPtr createAggregateFunctionTimeseries(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings, MakeFunction && make_function, bool has_grid_argument = false)
{
    checkTimeseriesAggregateFunctionsEnabled(name, settings);
    auto [timestamp_type, value_type] = getTimeseriesTimestampAndValueTypes(name, argument_types, has_grid_argument);
    return createAggregateFunctionTimeseriesWithTimestampAndValueTypes(name, parameters, timestamp_type, value_type, make_function);
}

}
