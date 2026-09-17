#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesInstantValue.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesInstantValue(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesInstantValue(AggregateFunctionFactory & factory)
{
    /// timeSeriesInstantRateToGrid documentation
    FunctionDocumentation::Description description_timeSeriesInstantRateToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like irate](https://prometheus.io/docs/prometheus/latest/querying/functions/#irate) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `irate` are considered within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesInstantRateToGrid = R"(
timeSeriesInstantRateToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesInstantRateToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesInstantRateToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesInstantRateToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesInstantRateToGrid = {"Returns irate values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the instant rate value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesInstantRateToGrid = {
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
SELECT timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,0.2,0.1,0.1,NULL,NULL,0.3]                                                       │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Using array arguments",
        R"(
SET enable_time_series_aggregate_functions = 1;
-- it is possible to pass multiple samples of timestamps and values as Arrays of equal size
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesInstantRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,0.2,0.1,0.1,NULL,NULL,0.3]                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesInstantRateToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesInstantRateToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesInstantRateToGrid = {description_timeSeriesInstantRateToGrid, syntax_timeSeriesInstantRateToGrid, arguments_timeSeriesInstantRateToGrid, parameters_timeSeriesInstantRateToGrid, returned_value_timeSeriesInstantRateToGrid, examples_timeSeriesInstantRateToGrid, introduced_in_timeSeriesInstantRateToGrid, category_timeSeriesInstantRateToGrid};

    factory.registerFunction("timeSeriesInstantRateToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesInstantRateToGrid<TimestampType, IntervalType, ValueType>>(argument_types, parameters, start, end, step, window, scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesInstantRateToGrid});

    /// timeSeriesInstantDeltaToGrid documentation
    FunctionDocumentation::Description description_timeSeriesInstantDeltaToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like idelta](https://prometheus.io/docs/prometheus/latest/querying/functions/#idelta) from this data on a regular time grid described by start timestamp, end timestamp and step.
For each point on the grid the samples for calculating `idelta` are considered within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesInstantDeltaToGrid = R"(
timeSeriesInstantDeltaToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesInstantDeltaToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesInstantDeltaToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesInstantDeltaToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesInstantDeltaToGrid = {"Returns idelta values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the instant delta value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesInstantDeltaToGrid = {
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
SELECT timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,2,1,1,NULL,NULL,3]                                                                │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Using array arguments",
        R"(
SET enable_time_series_aggregate_functions = 1;
-- it is possible to pass multiple samples of timestamps and values as Arrays of equal size
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesInstantDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,2,1,1,NULL,NULL,3]                                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesInstantDeltaToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesInstantDeltaToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesInstantDeltaToGrid = {description_timeSeriesInstantDeltaToGrid, syntax_timeSeriesInstantDeltaToGrid, arguments_timeSeriesInstantDeltaToGrid, parameters_timeSeriesInstantDeltaToGrid, returned_value_timeSeriesInstantDeltaToGrid, examples_timeSeriesInstantDeltaToGrid, introduced_in_timeSeriesInstantDeltaToGrid, category_timeSeriesInstantDeltaToGrid};

    factory.registerFunction("timeSeriesInstantDeltaToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesInstantDeltaToGrid<TimestampType, IntervalType, ValueType>>(argument_types, parameters, start, end, step, window, scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesInstantDeltaToGrid});
}

}
