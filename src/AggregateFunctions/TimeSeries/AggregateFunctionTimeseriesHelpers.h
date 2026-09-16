#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>

#include <string_view>


namespace DB
{

class AggregateFunctionFactory;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Machinery shared by the `timeSeries*ToGrid` aggregate functions.
/// The nested namespace is required: `createWithValueType` and `createWithTimestampAndValueTypes` also name
/// unrelated helpers in AggregateFunctionTimeSeriesGroupArray.cpp and AggregateFunctionLast2Samples.cpp.
namespace TimeSeriesToGrid
{

/// Extracts a timestamp parameter value and converts it to decimal with the target scale (scale of the timestamp column)
DateTime64 extractTimestampParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale);

/// Extracts a duration parameter value and converts it to decimal with the target scale (scale of the timestamp column)
Decimal64 extractDurationParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale);

UInt64 extractIntParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);

Float64 extractFloatParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);

/// Validates that the aggregate function got exactly the expected number of parameters.
void assertParametersCount(const std::string & name, const Array & parameters, size_t required, std::string_view parameter_names);

/// These functions are in private preview: creating one throws unless a time series setting enables it.
void assertTimeSeriesAggregateFunctionsEnabled(const std::string & name, const Settings * settings);

/// With the timestamp, interval and value types resolved, parse the grid parameters and build the function via
/// the factory. The factory receives the parsed values and is the only place that knows the concrete function.
template <
    typename TimestampType,
    typename IntervalType,
    typename ValueType,
    typename MakeFunction
>
AggregateFunctionPtr createWithTypes(const std::string & name, const Array & parameters, UInt32 target_scale, MakeFunction && make_function)
{
    if constexpr (std::is_same_v<TimestampType, DateTime64>)
    {
        /// Convert start, end, step and staleness parameters to the scale of the timestamp column
        DateTime64 start_timestamp = extractTimestampParameter(name, "start", parameters[0], target_scale);
        DateTime64 end_timestamp = extractTimestampParameter(name, "end", parameters[1], target_scale);
        DateTime64 step = extractDurationParameter(name, "step", parameters[2], target_scale);
        DateTime64 window = extractDurationParameter(name, "window", parameters[3], target_scale);
        return make_function.template operator()<TimestampType, IntervalType, ValueType>(start_timestamp, end_timestamp, step, window, target_scale);
    }
    else
    {
        UInt64 start_timestamp = extractIntParameter(name, "start", parameters[0]);
        UInt64 end_timestamp = extractIntParameter(name, "end", parameters[1]);
        Int64 step = extractIntParameter(name, "step", parameters[2]);
        Int64 window = extractIntParameter(name, "window", parameters[3]);
        return make_function.template operator()<TimestampType, IntervalType, ValueType>(
            static_cast<TimestampType>(start_timestamp), static_cast<TimestampType>(end_timestamp),
            static_cast<IntervalType>(step), static_cast<IntervalType>(window), target_scale);
    }
}

/// Resolves the timestamp and interval types from the timestamp type, then delegates to createWithTypes.
template <
    typename ValueType,
    typename MakeFunction
>
AggregateFunctionPtr createWithValueType(const std::string & name, const Array & parameters, const DataTypePtr & timestamp_type, MakeFunction && make_function)
{
    if (isDateTime64(timestamp_type))
    {
        auto timestamp_decimal = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type);
        return createWithTypes<DateTime64, Int64, ValueType>(name, parameters, timestamp_decimal->getScale(), make_function);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        return createWithTypes<UInt32, Int32, ValueType>(name, parameters, 0, make_function);
    }

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of the timestamps for aggregate function {}",
                    timestamp_type->getName(), name);
}

/// Resolves the value type, then delegates to createWithValueType.
template <typename MakeFunction>
AggregateFunctionPtr createWithTimestampAndValueTypes(const std::string & name, const Array & parameters,
    const DataTypePtr & timestamp_type, const DataTypePtr & value_type, MakeFunction && make_function)
{
    if (value_type->getTypeId() == TypeIndex::Float64)
        return createWithValueType<Float64>(name, parameters, timestamp_type, make_function);
    if (value_type->getTypeId() == TypeIndex::Float32)
        return createWithValueType<Float32>(name, parameters, timestamp_type, make_function);

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
    assertTimeSeriesAggregateFunctionsEnabled(name, settings);

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

        return createWithTimestampAndValueTypes(
            name, parameters, tuple_type->getElements()[0], tuple_type->getElements()[1], make_function);
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
        return createWithTimestampAndValueTypes(name, parameters, timestamp_type, value_type, make_function);
    }

    return createWithTimestampAndValueTypes(name, parameters, sample_types[0], sample_types[1], make_function);
}

}

/// One registration function per traits header, each in its own translation unit. Every registration instantiates
/// `AggregateFunctionTimeseries<Traits, ...>` for 2 value types x 2 timestamp types, so a unit holding all of them
/// exceeds the 50 MB per-object limit that utils/check-large-objects.sh enforces.
void registerAggregateFunctionTimeseriesExtrapolatedValue(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesInstantValue(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesLinearRegression(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesChanges(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesToGridSparse(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesCompensatedSum(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesCount(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesMax(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesMin(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesPresentToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesQuantileToGrid(AggregateFunctionFactory & factory);

}
