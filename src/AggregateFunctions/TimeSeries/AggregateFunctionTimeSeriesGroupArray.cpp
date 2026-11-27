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
    FunctionDocumentation::Description description = R"(
Sorts time series data by timestamp in ascending order.

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
timeSeriesGroupArray(timestamp, value)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"timestamp", "Timestamp of the sample.", {"DateTime", "UInt32", "UInt64"}},
        {"value", "Value of the time series corresponding to the timestamp.", {"(U)Int*", "Float*", "Decimal"}},
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of tuples `(timestamp, value)` sorted by timestamp in ascending order. If there are multiple values for the same timestamp then the function chooses the greatest of these values.", {"Array(Tuple(T1, T2))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage with individual values",
        R"(
WITH
    [110, 120, 130, 140, 140, 100]::Array(UInt32) AS timestamps,
    [1, 6, 8, 17, 19, 5]::Array(Float32) AS values
SELECT timeSeriesGroupArray(timestamp, value)
FROM
(
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesGroupArray(timestamp, value)───────────────┐
│ [(100, 5), (110, 1), (120, 6), (130, 8), (140, 19)]  │
└──────────────────────────────────────────────────────┘
        )"
    },
    {
        "Passing multiple samples of timestamps and values as arrays of equal size",
        R"(
WITH
    [110, 120, 130, 140, 140, 100]::Array(UInt32) AS timestamps,
    [1, 6, 8, 17, 19, 5]::Array(Float32) AS values
SELECT timeSeriesGroupArray(timestamps, values);
        )",
        R"(
┌─timeSeriesGroupArray(timestamps, values)──────────────┐
│ [(100, 5), (110, 1), (120, 6), (130, 8), (140, 19)]   │
└───────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesGroupArray", {createAggregateFunctionTimeseriesGroupArray, documentation});
}

}
