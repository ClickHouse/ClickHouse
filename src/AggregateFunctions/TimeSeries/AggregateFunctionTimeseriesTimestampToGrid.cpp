#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesTimestampToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesTimestampToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesTimestampToGrid(AggregateFunctionFactory & factory)
{
    /// timeSeriesTimestampToGrid documentation
    FunctionDocumentation::Description description_timeSeriesTimestampToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and, for each point on a regular time grid described by start timestamp, end timestamp and step, returns the timestamp (in seconds since epoch) of the most recent sample within the specified time window. Used to implement [PromQL's `timestamp()` function](https://prometheus.io/docs/prometheus/latest/querying/functions/#timestamp).

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesTimestampToGrid = R"(
timeSeriesTimestampToGrid(start_timestamp, end_timestamp, grid_step, staleness_window)(timestamp, value)
timeSeriesTimestampToGrid(start_timestamp, end_timestamp, grid_step, staleness_window)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesTimestampToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness_window", "Specifies the maximum staleness of the most recent sample in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesTimestampToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesTimestampToGrid = {"Returns the timestamp (in seconds since epoch) of the most recent sample within the window, for each point of the specified grid. The returned array contains one value for each time grid point. The value is NULL if there is no sample within the window for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesTimestampToGrid = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    30 AS window_seconds  -- "staleness" window
SELECT timeSeriesTimestampToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesTimestampToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,120,130,140,140,NULL,190,210]                                                    │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesTimestampToGrid = {26, 8};
    FunctionDocumentation::Category category_timeSeriesTimestampToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesTimestampToGrid = {description_timeSeriesTimestampToGrid, syntax_timeSeriesTimestampToGrid, arguments_timeSeriesTimestampToGrid, parameters_timeSeriesTimestampToGrid, returned_value_timeSeriesTimestampToGrid, examples_timeSeriesTimestampToGrid, introduced_in_timeSeriesTimestampToGrid, category_timeSeriesTimestampToGrid};

    factory.registerFunction("timeSeriesTimestampToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesTimestampToGrid<TimestampType, IntervalType, ValueType>>(argument_types, parameters, start, end, step, window, scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesTimestampToGrid});
}

}
