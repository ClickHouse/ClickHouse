#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesFirstToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesFirstToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesFirstToGrid(AggregateFunctionFactory & factory)
{
    /// timeSeriesFirstToGrid documentation
    FunctionDocumentation::Description description_timeSeriesFirstToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like first_over_time](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the result is the value of the earliest sample within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesFirstToGrid = R"(
timeSeriesFirstToGrid(start_timestamp, end_timestamp, grid_step, staleness_window)(timestamp, value)
timeSeriesFirstToGrid(start_timestamp, end_timestamp, grid_step, staleness_window)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesFirstToGrid = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness_window", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesFirstToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesFirstToGrid = {"Returns the values of the earliest samples on the specified grid, of the same type as `value`. The returned array contains one value for each time grid point. The value is NULL if there is no sample within the window for a particular grid point.", {"Array(Nullable(Float*))"}};
    FunctionDocumentation::Examples examples_timeSeriesFirstToGrid = {
    {
        "Calculate first_over_time values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210]",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to staleness window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    30 AS window_seconds  -- "staleness" window
SELECT timeSeriesFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,1,1,3,4,NULL,5,5]                                                            │
└─────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Same query with array arguments",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    30 AS window_seconds
SELECT timeSeriesFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,1,1,3,4,NULL,5,5]                                                              │
└───────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesFirstToGrid = {26, 10};
    FunctionDocumentation::Category category_timeSeriesFirstToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesFirstToGrid = {description_timeSeriesFirstToGrid, syntax_timeSeriesFirstToGrid, arguments_timeSeriesFirstToGrid, parameters_timeSeriesFirstToGrid, returned_value_timeSeriesFirstToGrid, examples_timeSeriesFirstToGrid, introduced_in_timeSeriesFirstToGrid, category_timeSeriesFirstToGrid};

    factory.registerFunction("timeSeriesFirstToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesFirstToGrid<TimestampType, ValueType, /* return_timestamp = */ false>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesFirstToGrid});

    /// timeSeriesTimestampOfFirstToGrid documentation
    FunctionDocumentation::Description description_timeSeriesTimestampOfFirstToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like ts_of_first_over_time](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the result is the timestamp of the earliest sample within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesTimestampOfFirstToGrid = R"(
timeSeriesTimestampOfFirstToGrid(start_timestamp, end_timestamp, grid_step, staleness_window)(timestamp, value)
timeSeriesTimestampOfFirstToGrid(start_timestamp, end_timestamp, grid_step, staleness_window)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesTimestampOfFirstToGrid = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness_window", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesTimestampOfFirstToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesTimestampOfFirstToGrid = {"Returns the timestamps of the earliest samples on the specified grid, of the same type as `timestamp`. The returned array contains one value for each time grid point. The value is NULL if there is no sample within the window for a particular grid point.", {"Array(Nullable(UInt32))", "Array(Nullable(DateTime))", "Array(Nullable(DateTime64))"}};
    FunctionDocumentation::Examples examples_timeSeriesTimestampOfFirstToGrid = {
    {
        "Calculate ts_of_first_over_time values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210]",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to staleness window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    30 AS window_seconds  -- "staleness" window
SELECT timeSeriesTimestampOfFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesTimestampOfFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)───────────────────────────────────────────────────┐
│ [NULL,NULL,'1970-01-01 00:01:50','1970-01-01 00:01:50','1970-01-01 00:02:10','1970-01-01 00:02:20',NULL,'1970-01-01 00:03:10','1970-01-01 00:03:10'] │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Same query with array arguments",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    30 AS window_seconds
SELECT timeSeriesTimestampOfFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesTimestampOfFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─────────────────────────────────────────────────┐
│ [NULL,NULL,'1970-01-01 00:01:50','1970-01-01 00:01:50','1970-01-01 00:02:10','1970-01-01 00:02:20',NULL,'1970-01-01 00:03:10','1970-01-01 00:03:10'] │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesTimestampOfFirstToGrid = {26, 10};
    FunctionDocumentation::Category category_timeSeriesTimestampOfFirstToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesTimestampOfFirstToGrid = {description_timeSeriesTimestampOfFirstToGrid, syntax_timeSeriesTimestampOfFirstToGrid, arguments_timeSeriesTimestampOfFirstToGrid, parameters_timeSeriesTimestampOfFirstToGrid, returned_value_timeSeriesTimestampOfFirstToGrid, examples_timeSeriesTimestampOfFirstToGrid, introduced_in_timeSeriesTimestampOfFirstToGrid, category_timeSeriesTimestampOfFirstToGrid};

    factory.registerFunction("timeSeriesTimestampOfFirstToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesFirstToGrid<TimestampType, ValueType, /* return_timestamp = */ true>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesTimestampOfFirstToGrid});
}

}
