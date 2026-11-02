#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesCount.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesCount(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesCount(AggregateFunctionFactory & factory)
{
    /// timeSeriesCountToGrid documentation
    FunctionDocumentation::Description description_timeSeriesCountToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like count_over_time](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the number of samples within the specified time window is counted.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesCountToGrid = R"(
timeSeriesCountToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesCountToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesCountToGrid = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesCountToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesCountToGrid = {"Returns the count of samples on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window for a particular grid point.", {"Array(Nullable(UInt64))"}};
    FunctionDocumentation::Examples examples_timeSeriesCountToGrid = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesCountToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesCountToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,2,3,4,2,1,1,3]                                                               │
└─────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Using array arguments",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesCountToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesCountToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,2,3,4,2,1,1,3]                                                                 │
└───────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesCountToGrid = {26, 9};
    FunctionDocumentation::Category category_timeSeriesCountToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesCountToGrid = {description_timeSeriesCountToGrid, syntax_timeSeriesCountToGrid, arguments_timeSeriesCountToGrid, parameters_timeSeriesCountToGrid, returned_value_timeSeriesCountToGrid, examples_timeSeriesCountToGrid, introduced_in_timeSeriesCountToGrid, category_timeSeriesCountToGrid};

    factory.registerFunction("timeSeriesCountToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesCount<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesCountToGrid});
}

}
