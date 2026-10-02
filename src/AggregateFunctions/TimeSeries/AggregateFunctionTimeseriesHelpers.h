#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/IDataType.h>

#include <algorithm>
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

/// Throws if the time series aggregate functions are disabled by the settings.
void checkTimeseriesAggregateFunctionsEnabled(const std::string & name, const Settings * settings);

/// Validates that the aggregate function got exactly the expected number of parameters.
void assertTimeseriesParametersCount(const std::string & name, const Array & parameters, size_t expected_parameter_count, std::string_view parameter_names);

/// Extracts a timestamp parameter value and converts it to decimal with the target scale (scale of the timestamp column)
DateTime64 extractTimeseriesTimestampParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale);

/// Extracts a duration parameter value and converts it to decimal with the target scale (scale of the timestamp column)
Decimal64 extractTimeseriesDurationParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale);

/// Extracts a floating-point parameter value from a number (Float, Decimal or integer) or from a string containing a number.
Float64 extractTimeseriesFloatParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);

/// Returns the scale of the grid: the greatest scale among the Decimal parameters of the grid (start, end, step, window),
/// but at least 3 (milliseconds). Parameters of other types (integers, floats, strings) don't affect the scale.
UInt32 getTimeseriesParametersScale(const Array & parameters);

/// Validates the argument types of a timeSeries*ToGrid function and returns the timestamp and value types of the samples.
/// `num_extra_arguments` is the number of arguments following the samples (see `AggregateFunctionTimeseriesBase::num_extra_arguments`),
/// their types are checked by the function itself.
std::pair<DataTypePtr, DataTypePtr> getTimeseriesTimestampAndValueTypes(const std::string & name, const DataTypes & argument_types, size_t num_extra_arguments);


/// With the value type and the type of the timestamps in the input columns resolved, parses the grid parameters and builds
/// the function via the factory. The factory receives the parsed values and is the only place that knows the concrete function.
/// The grid uses DateTime64 timestamps and Decimal64 intervals with the scale `grid_scale`, which is not less than the scale
/// of the timestamps in the input columns (`column_timestamp_scale`), so the input timestamps are converted to the grid exactly.
template <
    typename TimestampType,
    typename ValueType,
    typename MakeFunction
>
AggregateFunctionPtr createAggregateFunctionTimeseriesWithTypes(const std::string & name, const Array & parameters, UInt32 grid_scale, UInt32 column_timestamp_scale, MakeFunction && make_function)
{
    DateTime64 grid_start = extractTimeseriesTimestampParameter(name, "start", parameters[0], grid_scale);
    DateTime64 grid_end = extractTimeseriesTimestampParameter(name, "end", parameters[1], grid_scale);
    Decimal64 grid_step = extractTimeseriesDurationParameter(name, "step", parameters[2], grid_scale);
    Decimal64 window = extractTimeseriesDurationParameter(name, "window", parameters[3], grid_scale);
    return make_function.template operator()<TimestampType, ValueType>(
        grid_start, grid_end, grid_step, window, grid_scale, column_timestamp_scale);
}

/// Resolves the type of the timestamps in the input columns and the scale of the grid, then delegates to createAggregateFunctionTimeseriesWithTypes.
template <
    typename ValueType,
    typename MakeFunction
>
AggregateFunctionPtr createAggregateFunctionTimeseriesWithValueType(const std::string & name, const Array & parameters, const DataTypePtr & timestamp_type, MakeFunction && make_function)
{
    const UInt32 parameters_scale = getTimeseriesParametersScale(parameters);

    if (isDateTime64(timestamp_type))
    {
        const UInt32 column_timestamp_scale = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type)->getScale();
        return createAggregateFunctionTimeseriesWithTypes<DateTime64, ValueType>(
            name, parameters, std::max(column_timestamp_scale, parameters_scale), column_timestamp_scale, make_function);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        /// Both DateTime and UInt32 timestamps are stored in ColumnUInt32.
        return createAggregateFunctionTimeseriesWithTypes<UInt32, ValueType>(name, parameters, parameters_scale, /* column_timestamp_scale = */ 0, make_function);
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
/// builds the function through the given factory (a generic lambda templated on the timestamp and value types of the samples).
/// `num_extra_arguments` is the number of arguments following the samples (see `AggregateFunctionTimeseriesBase::num_extra_arguments`).
template <typename MakeFunction>
AggregateFunctionPtr createAggregateFunctionTimeseries(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings, MakeFunction && make_function, size_t num_extra_arguments = 0)
{
    checkTimeseriesAggregateFunctionsEnabled(name, settings);
    auto [timestamp_type, value_type] = getTimeseriesTimestampAndValueTypes(name, argument_types, num_extra_arguments);
    return createAggregateFunctionTimeseriesWithTimestampAndValueTypes(name, parameters, timestamp_type, value_type, make_function);
}

}
