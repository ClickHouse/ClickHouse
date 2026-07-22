#pragma once

#include <memory>
#include <string_view>
#include <type_traits>

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/IDataType.h>


namespace DB
{

class AggregateFunctionFactory;

Decimal64 normalizeParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale);
UInt64 extractIntParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);
Float64 extractFloatParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);
void assertTimeseriesParametersCount(const std::string & name, const Array & parameters, size_t required, std::string_view parameter_names);

void registerAggregateFunctionTimeseriesOverTimeGrid(AggregateFunctionFactory & factory);

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}

/// Shared factory for `timeSeries*ToGrid` aggregates. `make_function` is a generic lambda templated on
/// timestamp, interval and value types; it receives the parsed grid parameters and timestamp scale.
template <
    typename TimestampType,
    typename IntervalType,
    typename ValueType,
    typename MakeFunction>
AggregateFunctionPtr createTimeseriesWithTypes(const std::string & name, const Array & parameters, UInt32 target_scale, MakeFunction && make_function)
{
    if constexpr (std::is_same_v<TimestampType, DateTime64>)
    {
        DateTime64 start_timestamp = normalizeParameter(name, "start", parameters[0], target_scale);
        DateTime64 end_timestamp = normalizeParameter(name, "end", parameters[1], target_scale);
        DateTime64 step = normalizeParameter(name, "step", parameters[2], target_scale);
        DateTime64 window = normalizeParameter(name, "window", parameters[3], target_scale);
        return make_function.template operator()<TimestampType, IntervalType, ValueType>(
            start_timestamp, end_timestamp, step, window, target_scale);
    }
    else
    {
        UInt64 start_timestamp = extractIntParameter(name, "start", parameters[0]);
        UInt64 end_timestamp = extractIntParameter(name, "end", parameters[1]);
        Int64 step = extractIntParameter(name, "step", parameters[2]);
        Int64 window = extractIntParameter(name, "window", parameters[3]);
        return make_function.template operator()<TimestampType, IntervalType, ValueType>(
            static_cast<TimestampType>(start_timestamp),
            static_cast<TimestampType>(end_timestamp),
            static_cast<IntervalType>(step),
            static_cast<IntervalType>(window),
            target_scale);
    }
}

template <typename ValueType, typename MakeFunction>
AggregateFunctionPtr createTimeseriesWithValueType(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, bool array_arguments, MakeFunction && make_function)
{
    const auto & timestamp_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType() : argument_types[0];

    if (isDateTime64(timestamp_type))
    {
        auto timestamp_decimal = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type);
        return createTimeseriesWithTypes<DateTime64, Int64, ValueType>(name, parameters, timestamp_decimal->getScale(), make_function);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        return createTimeseriesWithTypes<UInt32, Int32, ValueType>(name, parameters, 0, make_function);
    }

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                    timestamp_type->getName(), name);
}

template <typename MakeFunction>
AggregateFunctionPtr createAggregateFunctionTimeseries(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings, MakeFunction && make_function)
{
    if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
            name);

    assertBinary(name, argument_types);

    if ((argument_types[0]->getTypeId() == TypeIndex::Array) != (argument_types[1]->getTypeId() == TypeIndex::Array))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal combination of argument type {} and {} for aggregate function {}, expected both arguments to be arrays or not arrays",
            argument_types[0]->getName(), argument_types[1]->getName(), name);

    const bool array_arguments = argument_types[1]->getTypeId() == TypeIndex::Array;
    const auto & value_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types[1].get())->getNestedType() : argument_types[1];

    if (value_type->getTypeId() == TypeIndex::Float64)
        return createTimeseriesWithValueType<Float64>(name, argument_types, parameters, array_arguments, make_function);
    if (value_type->getTypeId() == TypeIndex::Float32)
        return createTimeseriesWithValueType<Float32>(name, argument_types, parameters, array_arguments, make_function);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
}

}
