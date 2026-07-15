#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesGroupArray.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}


namespace
{
    template <bool array_arguments, typename ValueType>
    AggregateFunctionPtr createWithValueType(const String & name, const DataTypes & argument_types)
    {
        const auto & timestamp_type = array_arguments ? typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType() : argument_types[0];

        AggregateFunctionPtr res;
        if (isDateTime64(timestamp_type))
        {
            res = std::make_shared<AggregateFunctionTimeSeriesGroupArray<DateTime64, ValueType, array_arguments>>(argument_types);
        }
        else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
        {
            res = std::make_shared<AggregateFunctionTimeSeriesGroupArray<UInt32, ValueType, array_arguments>>(argument_types);
        }

        if (!res)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                            timestamp_type->getName(), name);

        return res;
    }

    AggregateFunctionPtr createAggregateFunctionTimeseriesGroupArray(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
                name);

        assertNoParameters(name, parameters);
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
                res = createWithValueType<true, Float64>(name, argument_types);
            else
                res = createWithValueType<false, Float64>(name, argument_types);
        }
        else if (value_type->getTypeId() == TypeIndex::Float32)
        {
            if (array_arguments)
                res = createWithValueType<true, Float32>(name, argument_types);
            else
                res = createWithValueType<false, Float32>(name, argument_types);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
        }

        return res;
    }
}

void registerAggregateFunctionTimeseriesGroupArray(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesGroupArray", createAggregateFunctionTimeseriesGroupArray);
}

}
