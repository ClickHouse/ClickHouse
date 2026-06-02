#pragma once

#include <memory>

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

template <
    bool is_rate_or_resets,
    bool is_predict,
    bool is_quantile,
    bool array_arguments,
    typename ValueType,
    template <bool, typename, typename, typename, bool> class FunctionTraits,
    template <typename> class Function
>
AggregateFunctionPtr createWithValueType(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    static_assert(!(is_predict && is_quantile), "is_predict and is_quantile are mutually exclusive");

    const auto & timestamp_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType() : argument_types[0];

    if constexpr (is_predict)
    {
        if (parameters.size() != 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function {} requires 5 parameters: start_timestamp, end_timestamp, step, window, predict_offset", name);
    }
    else if constexpr (is_quantile)
    {
        if (parameters.size() != 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function {} requires 5 parameters: start_timestamp, end_timestamp, step, window, phi", name);
    }
    else if (parameters.size() != 4)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 4 parameters: start_timestamp, end_timestamp, step, window", name);
    }

    const Field & start_timestamp_param = parameters[0];
    const Field & end_timestamp_param = parameters[1];
    const Field & step_param = parameters[2];
    const Field & window_param = parameters[3];
    const Field & fifth_param_field = (is_predict || is_quantile) ? parameters[4] : Field();

    AggregateFunctionPtr res;
    if (isDateTime64(timestamp_type))
    {
        /// Convert start, end, step and staleness parameters to the scale of the timestamp column
        auto timestamp_decimal = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type);
        auto target_scale = timestamp_decimal->getScale();

        DateTime64 start_timestamp = normalizeParameter(name, "start", start_timestamp_param, target_scale);
        DateTime64 end_timestamp = normalizeParameter(name, "end", end_timestamp_param, target_scale);
        DateTime64 step = normalizeParameter(name, "step", step_param, target_scale);
        DateTime64 window = normalizeParameter(name, "window", window_param, target_scale);

        if constexpr (is_predict)
        {
            /// predict_offset is a duration in seconds and must be scaled to the timestamp's unit.
            Float64 predict_offset = extractFloatParameter(name, "predict_offset", fifth_param_field)
                * static_cast<Float64>(DecimalUtils::scaleMultiplier<Int64>(target_scale));
            res = std::make_shared<Function<FunctionTraits<array_arguments, DateTime64, Int64, ValueType, is_predict>>>
                (argument_types, start_timestamp, end_timestamp, step, window, target_scale, predict_offset);
        }
        else if constexpr (is_quantile)
        {
            /// Quantile level is dimensionless and must NOT be scaled with the timestamp unit.
            Float64 phi = extractFloatParameter(name, "phi", fifth_param_field);
            res = std::make_shared<Function<FunctionTraits<array_arguments, DateTime64, Int64, ValueType, false>>>
                (argument_types, start_timestamp, end_timestamp, step, window, target_scale, phi);
        }
        else
        {
            res = std::make_shared<Function<FunctionTraits<array_arguments, DateTime64, Int64, ValueType, is_rate_or_resets>>>
                (argument_types, start_timestamp, end_timestamp, step, window, target_scale);
        }
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        UInt64 start_timestamp = extractIntParameter(name, "start", start_timestamp_param);
        UInt64 end_timestamp = extractIntParameter(name, "end", end_timestamp_param);
        Int64 step = extractIntParameter(name, "step", step_param);
        Int64 window = extractIntParameter(name, "window", window_param);

        if constexpr (is_predict)
        {
            /// predict_offset is already in seconds (no scale for UInt32/DateTime).
            Float64 predict_offset = extractFloatParameter(name, "predict_offset", fifth_param_field);
            res = std::make_shared<Function<FunctionTraits<array_arguments, UInt32, Int32, ValueType, is_predict>>>
                (argument_types, start_timestamp, end_timestamp, step, window, 0, predict_offset);
        }
        else if constexpr (is_quantile)
        {
            Float64 phi = extractFloatParameter(name, "phi", fifth_param_field);
            res = std::make_shared<Function<FunctionTraits<array_arguments, UInt32, Int32, ValueType, false>>>
                (argument_types, start_timestamp, end_timestamp, step, window, 0, phi);
        }
        else
        {
            res = std::make_shared<Function<FunctionTraits<array_arguments, UInt32, Int32, ValueType, is_rate_or_resets>>>
                (argument_types, start_timestamp, end_timestamp, step, window, 0);
        }
    }

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                        timestamp_type->getName(), name);

    return res;
}

template <
    bool is_rate_or_resets,
    bool is_predict,
    bool is_quantile,
    template <bool, typename, typename, typename, bool> class FunctionTraits,
    template <typename> class Function
>
AggregateFunctionPtr createAggregateFunctionTimeseries(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
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

    AggregateFunctionPtr res;
    if (value_type->getTypeId() == TypeIndex::Float64)
    {
        if (array_arguments)
            res = createWithValueType<is_rate_or_resets, is_predict, is_quantile, true, Float64, FunctionTraits, Function>(name, argument_types, parameters);
        else
            res = createWithValueType<is_rate_or_resets, is_predict, is_quantile, false, Float64, FunctionTraits, Function>(name, argument_types, parameters);
    }
    else if (value_type->getTypeId() == TypeIndex::Float32)
    {
        if (array_arguments)
            res = createWithValueType<is_rate_or_resets, is_predict, is_quantile, true, Float32, FunctionTraits, Function>(name, argument_types, parameters);
        else
            res = createWithValueType<is_rate_or_resets, is_predict, is_quantile, false, Float32, FunctionTraits, Function>(name, argument_types, parameters);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
    }

    return res;
}

}
