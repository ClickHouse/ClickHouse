#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesExtrapolatedValue.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesExtrapolatedValue(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesExtrapolatedValue(AggregateFunctionFactory & factory)
{
    /// timeSeriesRateToGrid documentation
    FunctionDocumentation::Description description_timeSeriesRateToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like rate](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `rate` are considered within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesRateToGrid = R"(
timeSeriesRateToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesRateToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesRateToGrid = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesRateToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesRateToGrid = {"Returns rate values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the rate value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesRateToGrid = {
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
SELECT timeSeriesRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)──┐
│ [NULL,NULL,0,0.06666666666666667,0.1,0.05555555555555555,NULL,NULL,0.08333333333333333] │
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
SELECT timeSeriesRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesRateToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,0.06666666666666667,0.1,0.05555555555555555,NULL,NULL,0.08333333333333333]  │
└──────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesRateToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesRateToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesRateToGrid = {description_timeSeriesRateToGrid, syntax_timeSeriesRateToGrid, arguments_timeSeriesRateToGrid, parameters_timeSeriesRateToGrid, returned_value_timeSeriesRateToGrid, examples_timeSeriesRateToGrid, introduced_in_timeSeriesRateToGrid, category_timeSeriesRateToGrid};

    factory.registerFunction("timeSeriesRateToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesRateToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesRateToGrid});

    /// timeSeriesIncreaseToGrid documentation
    FunctionDocumentation::Description description_timeSeriesIncreaseToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like increase](https://prometheus.io/docs/prometheus/latest/querying/functions/#increase) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `increase` are considered within the specified time window.

The value is the total extrapolated increase of a counter over the window. A decrease between consecutive samples is treated as a counter reset and counted towards the increase, so the result reflects the cumulative growth of the counter even when it restarts from zero.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesIncreaseToGrid = R"(
timeSeriesIncreaseToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesIncreaseToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesIncreaseToGrid = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesIncreaseToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesIncreaseToGrid = {"Returns increase values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the increase value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesIncreaseToGrid = {
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
SELECT timeSeriesIncreaseToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesIncreaseToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,3,4.5,2.5,NULL,NULL,3.75]                                                     │
└────────────────────────────────────────────────────────────────────────────────────────────┘
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
SELECT timeSeriesIncreaseToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesIncreaseToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,3,4.5,2.5,NULL,NULL,3.75]                                                       │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesIncreaseToGrid = {26, 8};
    FunctionDocumentation::Category category_timeSeriesIncreaseToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesIncreaseToGrid = {description_timeSeriesIncreaseToGrid, syntax_timeSeriesIncreaseToGrid, arguments_timeSeriesIncreaseToGrid, parameters_timeSeriesIncreaseToGrid, returned_value_timeSeriesIncreaseToGrid, examples_timeSeriesIncreaseToGrid, introduced_in_timeSeriesIncreaseToGrid, category_timeSeriesIncreaseToGrid};

    factory.registerFunction("timeSeriesIncreaseToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesIncreaseToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesIncreaseToGrid});

    /// timeSeriesDeltaToGrid documentation
    FunctionDocumentation::Description description_timeSeriesDeltaToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like delta](https://prometheus.io/docs/prometheus/latest/querying/functions/#delta) from this data on a regular time grid described by start timestamp, end timestamp and step.
For each point on the grid the samples for calculating `delta` are considered within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesDeltaToGrid = R"(
timeSeriesDeltaToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesDeltaToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesDeltaToGrid = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesDeltaToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesDeltaToGrid = {"Returns delta values on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the delta value for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesDeltaToGrid = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,      -- start of timestamp grid
    90 + 120 AS end_ts,  -- end of timestamp grid
    15 AS step_seconds,  -- step of timestamp grid
    45 AS window_seconds -- "staleness" window
SELECT timeSeriesDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,3,4.5,2.5,NULL,NULL,3.75]                                                  │
└─────────────────────────────────────────────────────────────────────────────────────────┘
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
SELECT timeSeriesDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesDeltaToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,3,4.5,2.5,NULL,NULL,3.75]                                                    │
└───────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesDeltaToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesDeltaToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesDeltaToGrid = {description_timeSeriesDeltaToGrid, syntax_timeSeriesDeltaToGrid, arguments_timeSeriesDeltaToGrid, parameters_timeSeriesDeltaToGrid, returned_value_timeSeriesDeltaToGrid, examples_timeSeriesDeltaToGrid, introduced_in_timeSeriesDeltaToGrid, category_timeSeriesDeltaToGrid};

    factory.registerFunction("timeSeriesDeltaToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesDeltaToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesDeltaToGrid});
}

}
