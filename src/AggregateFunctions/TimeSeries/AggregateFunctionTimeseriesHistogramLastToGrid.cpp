#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHistogramLastToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHistogramHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesHistogramLastToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesHistogramLastToGrid(AggregateFunctionFactory & factory)
{
    /// timeSeriesHistogramLastToGrid documentation
    FunctionDocumentation::Description description_timeSeriesHistogramLastToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and native histograms and re-samples this data to a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the most recent (within the specified time window) histogram sample is chosen.

The histogram argument is the payload tuple of the `histograms` target table of a `TimeSeries` table: `Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64))`.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesHistogramLastToGrid = R"(
timeSeriesHistogramLastToGrid(start_timestamp, end_timestamp, grid_step, staleness_window)(timestamp, histogram)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesHistogramLastToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness_window", "Specifies the maximum staleness of the most recent sample in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesHistogramLastToGrid = {
        {"timestamp", "Timestamp of the sample.", {"UInt32", "DateTime", "DateTime64"}},
        {"histogram", "Native histogram sample corresponding to the timestamp: the payload tuple of the `histograms` target table.", {"Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesHistogramLastToGrid = {"Returns native histograms re-sampled to the specified grid. The returned array contains one value for each time grid point: the payload tuple of the most recent sample within the staleness window. The value is NULL if there is no sample for a particular grid point.", {"Array(Nullable(Tuple))"}};
    FunctionDocumentation::Examples examples_timeSeriesHistogramLastToGrid = {
    {
        "Basic usage with individual timestamp-histogram pairs",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    (0, 0, 0., 1., 2., 0., [], [], [], [], [], 1, 0, [], [])::Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64)) AS histogram,
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    30 AS window_seconds  -- "staleness" window
SELECT timeSeriesHistogramLastToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, histogram)
FROM
(
    SELECT arrayJoin([110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime)) AS timestamp
);
        )",
        R"(
┌─timeSeriesHistogramLastToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, histogram)────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [NULL,NULL,(0,0,0,1,2,0,[],[],[],[],[],0,0,[],[]),(0,0,0,1,2,0,[],[],[],[],[],0,0,[],[]),(0,0,0,1,2,0,[],[],[],[],[],0,0,[],[]),(0,0,0,1,2,0,[],[],[],[],[],0,0,[],[]),NULL,(0,0,0,1,2,0,[],[],[],[],[],0,0,[],[]),(0,0,0,1,2,0,[],[],[],[],[],0,0,[],[])] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesHistogramLastToGrid = {26, 8};
    FunctionDocumentation::Category category_timeSeriesHistogramLastToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesHistogramLastToGrid = {description_timeSeriesHistogramLastToGrid, syntax_timeSeriesHistogramLastToGrid, arguments_timeSeriesHistogramLastToGrid, parameters_timeSeriesHistogramLastToGrid, returned_value_timeSeriesHistogramLastToGrid, examples_timeSeriesHistogramLastToGrid, introduced_in_timeSeriesHistogramLastToGrid, category_timeSeriesHistogramLastToGrid};

    factory.registerFunction("timeSeriesHistogramLastToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesHistogramLastToGrid<TimestampType, IntervalType>>(argument_types, parameters, start, end, step, window, scale);
            };
            return createAggregateFunctionTimeseriesHistogram(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesHistogramLastToGrid});
}

}
