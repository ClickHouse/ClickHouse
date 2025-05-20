#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTimeseriesToGrid.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/IDataType.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_ts_to_grid_aggregate_function;
}

/// Extracts integer or decimal parameter value and converts it to decimal with the target scale (scale of the timestamp column)
Decimal64 normalizeParameter(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field, Int64 target_scale_multiplier);

UInt64 extractIntParamater(const std::string & function_name, const std::string & parameter_name, const Field & parameter_field);

namespace
{

template <typename ValueType>
AggregateFunctionPtr createWithValueType(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    const auto & timestamp_type = argument_types[0];

    if (parameters.size() != 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Aggregate function {} requires 4 parameters: start_timestamp, end_timestamp, step, staleness", name);

    const Field & start_timestamp_param = parameters[0];
    const Field & end_timestamp_param = parameters[1];
    const Field & step_param = parameters[2];
    const Field & staleness_param = parameters[3];

    AggregateFunctionPtr res;
    if (isDateTime64(timestamp_type))
    {
        /// Convert start, end, step and staleness parameters to the scale of the timestamp column
        auto timestamp_decimal = std::dynamic_pointer_cast<const DataTypeDateTime64>(timestamp_type);
        auto target_scale_multiplier = timestamp_decimal->getScaleMultiplier();

        DateTime64 start_timestamp = normalizeParameter(name, "start", start_timestamp_param, target_scale_multiplier);
        DateTime64 end_timestamp = normalizeParameter(name, "end", end_timestamp_param, target_scale_multiplier);
        DateTime64 step = normalizeParameter(name, "step", step_param, target_scale_multiplier);
        DateTime64 staleness = normalizeParameter(name, "staleness", staleness_param, target_scale_multiplier);

        res = std::make_shared<AggregateFunctionTimeseriesToGrid<DateTime64, Int64, ValueType>>(argument_types, start_timestamp, end_timestamp, step, staleness);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        UInt64 start_timestamp = extractIntParamater(name, "start", start_timestamp_param);
        UInt64 end_timestamp = extractIntParamater(name, "end", end_timestamp_param);
        Int64 step = extractIntParamater(name, "step", step_param);
        Int64 staleness = extractIntParamater(name, "staleness", staleness_param);

        res = std::make_shared<AggregateFunctionTimeseriesToGrid<UInt32, Int32, ValueType>>(argument_types, start_timestamp, end_timestamp, step, staleness);
    }

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                        timestamp_type->getName(), name);

    return res;
}

AggregateFunctionPtr createAggregateFunctionTimeseriesToGrid(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    if (settings && (*settings)[Setting::allow_experimental_ts_to_grid_aggregate_function] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_ts_to_grid_aggregate_function",
            name);

    assertBinary(name, argument_types);
    const auto & value_type = argument_types[1];

    AggregateFunctionPtr res;
    if (value_type->getTypeId() == TypeIndex::Float64)
    {
        res = createWithValueType<Float64>(name, argument_types, parameters);
    }
    else if (value_type->getTypeId() == TypeIndex::Float32)
    {
        res = createWithValueType<Float32>(name, argument_types, parameters);
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
    }

    return res;
}

}

void registerAggregateFunctionTimeseriesToGrid(AggregateFunctionFactory & factory)
{
    factory.registerFunction("tsToGrid", createAggregateFunctionTimeseriesToGrid);
}

}
