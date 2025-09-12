#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTimeseriesInstantValue.h>
#include <AggregateFunctions/AggregateFunctionTimeseriesExtrapolatedValue.h>
#include <AggregateFunctions/AggregateFunctionTimeseriesToGridSparse.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/readDecimalText.h>
#include <Core/Settings.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// Extracts integer or decimal parameter value and converts it to decimal with the target scale (scale of the timestamp column)
Decimal64 normalizeParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, UInt32 target_scale)
{
    auto target_scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(target_scale);

    if (parameter_field.getType() == Field::Types::Decimal64)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal64>>();
        auto value_scale_multiplier = value.getScaleMultiplier();
        return (Decimal128(value.getValue()) * Decimal128(target_scale_multiplier)) / Decimal128(value_scale_multiplier);
    }
    else if (parameter_field.getType() == Field::Types::Decimal32)
    {
        auto value = parameter_field.safeGet<DecimalField<Decimal32>>();
        auto value_scale_multiplier = value.getScaleMultiplier();
        return Decimal64(value.getValue()) / value_scale_multiplier * target_scale_multiplier;
    }
    else if (Int64 int_value = 0; parameter_field.tryGet(int_value))
    {
        return Decimal64(int_value) * target_scale_multiplier;
    }
    else if (UInt64 uint_value = 0; parameter_field.tryGet(uint_value))
    {
        return Decimal64(uint_value) * target_scale_multiplier;
    }
    else if (String string_value; parameter_field.tryGet(string_value))
    {
        Decimal64 value{};
        UInt32 scale = target_scale;
        ReadBufferFromString buf(string_value);
        if (tryReadDecimalText(buf, value, 20, scale))
            return value * DecimalUtils::scaleMultiplier<Decimal64>(scale);
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

UInt64 extractIntParamater(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field)
{
    if (UInt64 int_value = 0; parameter_field.tryGet(int_value))
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


namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
}

namespace
{

template <
    bool is_rate,
    bool array_arguments,
    typename ValueType,
    template <bool, typename, typename, typename, bool> class FunctionTraits,
    template <typename> class Function
>
AggregateFunctionPtr createWithValueType(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    const auto & timestamp_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType() : argument_types[0];

    if (parameters.size() != 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Aggregate function {} requires 4 parameters: start_timestamp, end_timestamp, step, window", name);

    const Field & start_timestamp_param = parameters[0];
    const Field & end_timestamp_param = parameters[1];
    const Field & step_param = parameters[2];
    const Field & window_param = parameters[3];

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

        res = std::make_shared<Function<FunctionTraits<array_arguments, DateTime64, Int64, ValueType, is_rate>>>
            (argument_types, start_timestamp, end_timestamp, step, window, target_scale);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        UInt64 start_timestamp = extractIntParamater(name, "start", start_timestamp_param);
        UInt64 end_timestamp = extractIntParamater(name, "end", end_timestamp_param);
        Int64 step = extractIntParamater(name, "step", step_param);
        Int64 window = extractIntParamater(name, "window", window_param);

        res = std::make_shared<Function<FunctionTraits<array_arguments, UInt32, Int32, ValueType, is_rate>>>
                (argument_types, start_timestamp, end_timestamp, step, window, 0);
    }

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                        timestamp_type->getName(), name);

    return res;
}

template <
    bool is_rate,
    template <bool, typename, typename, typename, bool> class FunctionTraits,
    template <typename> class Function
>
AggregateFunctionPtr createAggregateFunctionTimeseries(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0)
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
            res = createWithValueType<is_rate, true, Float64, FunctionTraits, Function>(name, argument_types, parameters);
        else
            res = createWithValueType<is_rate, false, Float64, FunctionTraits, Function>(name, argument_types, parameters);
    }
    else if (value_type->getTypeId() == TypeIndex::Float32)
    {
        if (array_arguments)
            res = createWithValueType<is_rate, true, Float32, FunctionTraits, Function>(name, argument_types, parameters);
        else
            res = createWithValueType<is_rate, false, Float32, FunctionTraits, Function>(name, argument_types, parameters);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
    }

    return res;
}

}

void registerAggregateFunctionTimeseries(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesRateToGrid",
        createAggregateFunctionTimeseries<true, AggregateFunctionTimeseriesExtrapolatedValueTraits, AggregateFunctionTimeseriesExtrapolatedValue>);
    factory.registerFunction("timeSeriesDeltaToGrid",
        createAggregateFunctionTimeseries<false, AggregateFunctionTimeseriesExtrapolatedValueTraits, AggregateFunctionTimeseriesExtrapolatedValue>);

    factory.registerFunction("timeSeriesInstantRateToGrid",
        createAggregateFunctionTimeseries<true, AggregateFunctionTimeseriesInstantValueTraits, AggregateFunctionTimeseriesInstantValue>);
    factory.registerFunction("timeSeriesInstantDeltaToGrid",
        createAggregateFunctionTimeseries<false, AggregateFunctionTimeseriesInstantValueTraits, AggregateFunctionTimeseriesInstantValue>);

    factory.registerFunction("timeSeriesResampleToGridWithStaleness",
        createAggregateFunctionTimeseries<false, AggregateFunctionTimeseriesToGridSparseTraits, AggregateFunctionTimeseriesToGridSparse>);
}

}
