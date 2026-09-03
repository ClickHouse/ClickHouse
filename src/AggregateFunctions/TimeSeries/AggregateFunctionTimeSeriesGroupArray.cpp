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
    template <typename ValueType>
    AggregateFunctionPtr createWithValueType(const String & name, const DataTypes & argument_types, const DataTypePtr & timestamp_type)
    {
        AggregateFunctionPtr res;
        if (isDateTime64(timestamp_type))
        {
            res = std::make_shared<AggregateFunctionTimeSeriesGroupArray<DateTime64, ValueType>>(argument_types);
        }
        else if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
        {
            res = std::make_shared<AggregateFunctionTimeSeriesGroupArray<UInt32, ValueType>>(argument_types);
        }

        if (!res)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument (timestamp) for aggregate function {}",
                            timestamp_type->getName(), name);

        return res;
    }

    AggregateFunctionPtr createWithTimestampAndValueTypes(
        const String & name, const DataTypes & argument_types, const DataTypePtr & timestamp_type, const DataTypePtr & value_type)
    {
        if (value_type->getTypeId() == TypeIndex::Float64)
            return createWithValueType<Float64>(name, argument_types, timestamp_type);
        if (value_type->getTypeId() == TypeIndex::Float32)
            return createWithValueType<Float32>(name, argument_types, timestamp_type);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of 2nd argument (value) for aggregate function {}", value_type->getName(), name);
    }

    AggregateFunctionPtr createAggregateFunctionTimeseriesGroupArray(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
                name);

        assertNoParameters(name, parameters);

        if (argument_types.size() == 1)
        {
            /// The single argument form: Array(Tuple(timestamp, value)), which is the same as the result type.
            const auto * array_type = typeid_cast<const DataTypeArray *>(argument_types[0].get());
            const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
            if (!tuple_type || tuple_type->getElements().size() != 2)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument for aggregate function {}, expected Array(Tuple(timestamp, value))",
                    argument_types[0]->getName(), name);

            return createWithTimestampAndValueTypes(name, argument_types, tuple_type->getElements()[0], tuple_type->getElements()[1]);
        }

        assertBinary(name, argument_types);

        if ((argument_types[0]->getTypeId() == TypeIndex::Array) != (argument_types[1]->getTypeId() == TypeIndex::Array))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal combination of argument type {} and {} for aggregate function {}, expected both arguments to be arrays or not arrays",
                argument_types[0]->getName(), argument_types[1]->getName(), name);

        if (argument_types[1]->getTypeId() == TypeIndex::Array)
        {
            const auto & timestamp_type = typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType();
            const auto & value_type = typeid_cast<const DataTypeArray *>(argument_types[1].get())->getNestedType();
            return createWithTimestampAndValueTypes(name, argument_types, timestamp_type, value_type);
        }

        return createWithTimestampAndValueTypes(name, argument_types, argument_types[0], argument_types[1]);
    }
}

void registerAggregateFunctionTimeseriesGroupArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesGroupArray(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Sorts time series data by timestamp in ascending order.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series. This form has the same type as the result, which allows using the function in the `SimpleAggregateFunction` data type.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

:::note
This function is experimental, enable it by setting `allow_experimental_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
timeSeriesGroupArray(timestamp, value)
timeSeriesGroupArray(samples)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"timestamp", "Timestamp of the sample. Can be an individual value or an array.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be an individual value or an array.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above.", {"Array(Tuple(T1, T2))"}},
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of tuples `(timestamp, value)` sorted by timestamp in ascending order.", {"Array(Tuple(T1, T2))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage with individual values",
        R"(
SET allow_experimental_time_series_aggregate_functions = 1;
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
┌─timeSeriesGroupArray(timestamp, value)─────┐
│ [(100,5),(110,1),(120,6),(130,8),(140,19)] │
└────────────────────────────────────────────┘
        )"
    },
    {
        "Passing multiple samples of timestamps and values as arrays of equal size",
        R"(
SET allow_experimental_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 140, 140, 100]::Array(UInt32) AS timestamps,
    [1, 6, 8, 17, 19, 5]::Array(Float32) AS values
SELECT timeSeriesGroupArray(timestamps, values);
        )",
        R"(
┌─timeSeriesGroupArray(timestamps, values)───┐
│ [(100,5),(110,1),(120,6),(130,8),(140,19)] │
└────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesGroupArray", {createAggregateFunctionTimeseriesGroupArray, documentation});
}

}
