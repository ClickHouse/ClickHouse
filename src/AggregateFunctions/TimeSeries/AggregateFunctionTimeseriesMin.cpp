#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesMin.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesMin(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesMin(AggregateFunctionFactory & factory)
{
    /// timeSeriesMinToGrid documentation
    FunctionDocumentation::Description description_timeSeriesMinToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like min_over_time](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the result is the minimum of the sample values within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

Samples with NaN values are ignored unless all samples within the window are NaN.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesMinToGrid = R"(
timeSeriesMinToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesMinToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesMinToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesMinToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesMinToGrid = {"Returns the minimum of values on the specified grid, of the same type as `value`. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window for a particular grid point.", {"Array(Nullable(Float*))"}};
    FunctionDocumentation::Examples examples_timeSeriesMinToGrid = {
    {
        "Calculate min_over_time values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210, 225]",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    -- NOTE: the gap between 130 and 190 is to show how values are filled for ts = 180 according to window parameter
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 3, 2, 6, 6, 4, 2, 0]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 135 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesMinToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesMinToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,1,1,1,2,NULL,6,4,2]                                                        │
└───────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Same query with array arguments",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 3, 2, 6, 6, 4, 2, 0]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 135 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesMinToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesMinToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,1,1,1,2,NULL,6,4,2]                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesMinToGrid = {26, 9};
    FunctionDocumentation::Category category_timeSeriesMinToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesMinToGrid = {description_timeSeriesMinToGrid, syntax_timeSeriesMinToGrid, arguments_timeSeriesMinToGrid, parameters_timeSeriesMinToGrid, returned_value_timeSeriesMinToGrid, examples_timeSeriesMinToGrid, introduced_in_timeSeriesMinToGrid, category_timeSeriesMinToGrid};

    factory.registerFunction("timeSeriesMinToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesMinToGrid<TimestampType, IntervalType, ValueType>>(argument_types, parameters, start, end, step, window, scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesMinToGrid});

    /// timeSeriesTimestampOfMinToGrid documentation
    FunctionDocumentation::Description description_timeSeriesTimestampOfMinToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like ts_of_min_over_time](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the result is the timestamp in seconds of the latest sample with the minimum value within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

Samples with NaN values are ignored unless all samples within the window are NaN.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesTimestampOfMinToGrid = R"(
timeSeriesTimestampOfMinToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesTimestampOfMinToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesTimestampOfMinToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesTimestampOfMinToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesTimestampOfMinToGrid = {"`ts_of_min_over_time` values on the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window for a particular grid point.", {}};
    FunctionDocumentation::Examples examples_timeSeriesTimestampOfMinToGrid = {
    {
        "Calculate ts_of_min_over_time values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210, 225]",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    -- NOTE: the gap between 130 and 190 is to show how values are filled for ts = 180 according to window parameter
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 3, 2, 6, 6, 4, 2, 0]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 135 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesTimestampOfMinToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesTimestampOfMinToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,110,110,110,130,NULL,190,210,220]                                                     │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Same query with array arguments",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 3, 2, 6, 6, 4, 2, 0]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 135 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesTimestampOfMinToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesTimestampOfMinToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,110,110,110,130,NULL,190,210,220]                                                       │
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesTimestampOfMinToGrid = {26, 9};
    FunctionDocumentation::Category category_timeSeriesTimestampOfMinToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesTimestampOfMinToGrid = {description_timeSeriesTimestampOfMinToGrid, syntax_timeSeriesTimestampOfMinToGrid, arguments_timeSeriesTimestampOfMinToGrid, parameters_timeSeriesTimestampOfMinToGrid, returned_value_timeSeriesTimestampOfMinToGrid, examples_timeSeriesTimestampOfMinToGrid, introduced_in_timeSeriesTimestampOfMinToGrid, category_timeSeriesTimestampOfMinToGrid};

    factory.registerFunction("timeSeriesTimestampOfMinToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesTimestampOfMinToGrid<TimestampType, IntervalType, ValueType>>(argument_types, parameters, start, end, step, window, scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesTimestampOfMinToGrid});
}

}
