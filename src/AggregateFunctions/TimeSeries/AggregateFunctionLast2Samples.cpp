#include <AggregateFunctions/TimeSeries/AggregateFunctionLast2Samples.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/IDataType.h>
#include <Core/Settings.h>


namespace DB
{
struct Settings;
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}
namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
}
namespace
{

template <typename ValueType>
AggregateFunctionPtr createWithValueType(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    const auto & timestamp_type = argument_types[0];

    if (!parameters.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Aggregate function {} does not accept parameters", name);

    AggregateFunctionPtr res;
    if (isDateTime64(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<DateTime64, ValueType>>(argument_types);
    }
    if (isUInt64(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<UInt64, ValueType>>(argument_types);
    }
    if (isInt64(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<Int64, ValueType>>(argument_types);
    }
    else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<UInt32, ValueType>>(argument_types);
    }
    else if (isInt32(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<Int32, ValueType>>(argument_types);
    }
    else if (isUInt16(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<UInt16, ValueType>>(argument_types);
    }
    else if (isInt16(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<Int16, ValueType>>(argument_types);
    }
    else if (isUInt8(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<UInt8, ValueType>>(argument_types);
    }
    else if (isInt8(timestamp_type))
    {
        res = std::make_shared<AggregateFunctionLast2Samples<Int8, ValueType>>(argument_types);
    }

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
            timestamp_type->getName(), name);

    return res;
}

AggregateFunctionPtr createAggregateFunctionLast2Samples(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
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
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}",
            value_type->getName(), name);
    }

    return res;
}

}

void registerAggregateFunctionLast2Samples(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesLastTwoSamples", createAggregateFunctionLast2Samples);
}

}
