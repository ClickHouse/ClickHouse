#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesGroupArray.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesGroupArrayHistograms.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

    AggregateFunctionPtr createAggregateFunctionTimeSeriesGroupArrayHistograms(
        const String & name, const DataTypes & argument_types, const DataTypePtr & timestamp_type)
    {
        if (isDateTime64(timestamp_type))
            return std::make_shared<AggregateFunctionTimeSeriesGroupArrayHistograms<DateTime64>>(name, argument_types);
        if (isDateTime(timestamp_type) || isUInt32(timestamp_type))
            return std::make_shared<AggregateFunctionTimeSeriesGroupArrayHistograms<UInt32>>(name, argument_types);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of the `timestamp` element of the argument for aggregate function {}, expected DateTime64, DateTime or UInt32",
            timestamp_type->getName(), name);
    }

    AggregateFunctionPtr createAggregateFunctionTimeseriesGroupArray(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        /// The function is not gated by the `enable_time_series_aggregate_functions` setting: the TimeSeries table engine
        /// uses it in the `SimpleAggregateFunction` type of its inner tables, so the type must be parsed in any context.

        assertNoParameters(name, parameters);

        if (argument_types.size() == 1)
        {
            /// The single argument form: Array(Tuple(timestamp, value)), which is the same as the result type.
            const auto * array_type = typeid_cast<const DataTypeArray *>(argument_types[0].get());
            const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
            if (tuple_type && tuple_type->getElements().size() == 2)
                return createWithTimestampAndValueTypes(name, argument_types, tuple_type->getElements()[0], tuple_type->getElements()[1]);

            /// The implementation addresses the elements of a histogram sample by their positions in the registry, so the argument
            /// must have exactly the type of the `histograms` column of a TimeSeries table.
            if (const DataTypePtr histograms_timestamp_type = TimeSeriesHistogramsColumns::tryGetTimestampType(argument_types[0]))
                return createAggregateFunctionTimeSeriesGroupArrayHistograms(name, argument_types, histograms_timestamp_type);

            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function {}, expected Array(Tuple(timestamp, value)) or the type of the "
                "`histograms` column of a TimeSeries table: {}",
                argument_types[0]->getName(), name, TimeSeriesHistogramsColumns::describeArrayTypeForDocs("<timestamp type>"));
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

The samples can be passed in one of four forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series. This form has the same type as the result, which allows using the function in the `SimpleAggregateFunction` data type;
- as a single array of native histogram samples sorted by timestamp, where each row holds a whole time series. The array has the type of the `histograms` column of the samples tables of the `TimeSeries` table engine (`Array(Tuple(timestamp, flags, counter_reset_hint, schema, zero_threshold, sum, count_int, zero_count_int, count_float, zero_count_float, positive_int, negative_int, positive_float, negative_float, custom_values))`, see the documentation of the engine), which is also the type of the result, so this form can be used in the `SimpleAggregateFunction` data type too.

If several float samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

If several histogram samples have the same timestamp, only one of them is kept: the one with the greatest total count (`count_int` for an integer
histogram, `count_float` for a float one; a NaN count loses to any other count), on ties the one with the greatest `sum`, then the other
elements decide in the order of the tuple, starting with `flags` (a float histogram beats an integer one) and the gauge marker (a gauge
histogram beats a counter one). The other counter reset hints don't decide which sample is kept: of samples that differ only in them, the
kept one gets its hint detected again as described below or, if it is the first sample of the result, the common hint of these samples, or
an unknown one if their hints differ.
The winning histogram payload does not depend on the order of merging. Where merging changes a sample's predecessor or joins separate
input arrays, its counter reset hint is detected again from the data against the new predecessor by the rules of the inserts into a
`TimeSeries` table (a decrease of a count is a reset, a change of the schema or of the zero threshold makes the hint unknown), whatever
hint the sample had: that hint was about another predecessor. Gauge markers are kept. The hints of the other samples are kept as they are,
so the argument arrays must be normalized: every sample but the first of an array must carry the hint these rules detect against its
predecessor in the array, as the inserts into a `TimeSeries` table store them. Then the hints do not depend on the order of merging
either; for arrays with other hints they do.

<Note>
This function is in private preview.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = R"(
timeSeriesGroupArray(timestamp, value)
timeSeriesGroupArray(samples)
timeSeriesGroupArray(histograms)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"timestamp", "Timestamp of the sample. Can be an individual value or an array.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be an individual value or an array.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above.", {"Array(Tuple(T1, T2))"}},
        {"histograms", "Native histogram samples of the time series sorted by timestamp, with the type of the `histograms` column of a TimeSeries table and with normalized counter reset hints (see above).", {"Array(Tuple(DateTime64, ...))", "Array(Tuple(DateTime, ...))", "Array(Tuple(UInt32, ...))"}},
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of tuples `(timestamp, value)` sorted by timestamp in ascending order, or for `histograms` an array of the type of the argument sorted by timestamp.", {"Array(Tuple(T1, T2))", "Array(Tuple(T1, ...))"}};
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
┌─timeSeriesGroupArray(timestamp, value)─────┐
│ [(100,5),(110,1),(120,6),(130,8),(140,19)] │
└────────────────────────────────────────────┘
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
┌─timeSeriesGroupArray(timestamps, values)───┐
│ [(100,5),(110,1),(120,6),(130,8),(140,19)] │
└────────────────────────────────────────────┘
        )"
    },
    {
        "Merging two arrays of integer histograms",
        R"(
SELECT timeSeriesGroupArray(h)
FROM values('h Array(Tuple(timestamp UInt32, flags UInt8, counter_reset_hint UInt8, schema Int8, zero_threshold Float64, sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64, positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)), positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)), custom_values Array(Float64)))',
    [(100, 0, 0, 0, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], []), (120, 0, 0, 0, 0., 3., 3, 0, 0., 0., [(0, 2), (1, 1)], [], [], [], [])],
    [(110, 0, 0, 0, 0., 2., 2, 0, 0., 0., [(0, 1), (1, 1)], [], [], [], []), (120, 0, 0, 0, 0., 7., 4, 0, 0., 0., [(0, 3), (1, 1)], [], [], [], [])]);
        )",
        R"(
┌─timeSeriesGroupArray(h)───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [(100,0,0,0,0,1,1,0,0,0,[(0,1)],[],[],[],[]),(110,0,2,0,0,2,2,0,0,0,[(0,1),(1,1)],[],[],[],[]),(120,0,2,0,0,7,4,0,0,0,[(0,3),(1,1)],[],[],[],[])] │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesGroupArray", {createAggregateFunctionTimeseriesGroupArray, documentation});
}

}
