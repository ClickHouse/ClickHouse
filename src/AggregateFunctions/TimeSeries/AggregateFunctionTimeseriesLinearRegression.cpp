#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesLinearRegression.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>
#include <Core/DecimalFunctions.h>


namespace DB
{

void registerAggregateFunctionTimeseriesLinearRegression(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesLinearRegression(AggregateFunctionFactory & factory)
{
    /// timeSeriesDerivToGrid documentation
    FunctionDocumentation::Description description_timeSeriesDerivToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like derivative](https://prometheus.io/docs/prometheus/latest/querying/functions/#deriv) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `deriv` are considered within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Note>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Note>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesDerivToGrid = R"(
timeSeriesDerivToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesDerivToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesDerivToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesDerivToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesDerivToGrid = {"`deriv` values on the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the derivative value for a particular grid point.", {}};
    FunctionDocumentation::Examples examples_timeSeriesDerivToGrid = {
    {
        "Calculate derivative values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210]",
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
SELECT timeSeriesDerivToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
┌─timeSeriesDerivToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,0.1,0.11,0.1,NULL,NULL,0.15]                                               │
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
    45 AS window_seconds
SELECT timeSeriesDerivToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
        )",
        R"(
┌─timeSeriesDerivToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values)─┐
│ [NULL,NULL,0,0.1,0.11,0.1,NULL,NULL,0.15]                                                 │
└───────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesDerivToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesDerivToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesDerivToGrid = {description_timeSeriesDerivToGrid, syntax_timeSeriesDerivToGrid, arguments_timeSeriesDerivToGrid, parameters_timeSeriesDerivToGrid, returned_value_timeSeriesDerivToGrid, examples_timeSeriesDerivToGrid, introduced_in_timeSeriesDerivToGrid, category_timeSeriesDerivToGrid};

    factory.registerFunction("timeSeriesDerivToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesDerivToGrid<TimestampType, IntervalType, ValueType>>(argument_types, parameters, start, end, step, window, scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesDerivToGrid});

    /// timeSeriesPredictLinearToGrid documentation
    FunctionDocumentation::Description description_timeSeriesPredictLinearToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates a [PromQL-like linear prediction](https://prometheus.io/docs/prometheus/latest/querying/functions/#predict_linear) with a specified prediction timestamp offset from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `predict_linear` are considered within the specified time window.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Note>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Note>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesPredictLinearToGrid = R"(
timeSeriesPredictLinearToGrid(start_timestamp, end_timestamp, grid_step, staleness, predict_offset)(timestamp, value)
timeSeriesPredictLinearToGrid(start_timestamp, end_timestamp, grid_step, staleness, predict_offset)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesPredictLinearToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"predict_offset", "Specifies number of seconds of offset to add to prediction time.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesPredictLinearToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesPredictLinearToGrid = {"`predict_linear` values on the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there are not enough samples within the window to calculate the rate value for a particular grid point.", {}};
    FunctionDocumentation::Examples examples_timeSeriesPredictLinearToGrid = {
    {
        "Calculate predict_linear values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210] with a 60 second offset",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds, -- "staleness" window
    60 AS predict_offset  -- prediction time offset
SELECT timeSeriesPredictLinearToGrid(start_ts, end_ts, step_seconds, window_seconds, predict_offset)(timestamp, value)
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
┌─timeSeriesPredictLinearToGrid(start_ts, end_ts, step_seconds, window_seconds, predict_offset)(timestamp, value)─┐
│ [NULL,NULL,1,9.166667,11.6,12.5,NULL,NULL,16.5]                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
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
    45 AS window_seconds,
    60 AS predict_offset
SELECT timeSeriesPredictLinearToGrid(start_ts, end_ts, step_seconds, window_seconds, predict_offset)(timestamps, values);
        )",
        R"(
┌─timeSeriesPredictLinearToGrid(start_ts, end_ts, step_seconds, window_seconds, predict_offset)(timestamps, values)─┐
│ [NULL,NULL,1,9.166667,11.6,12.5,NULL,NULL,16.5]                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesPredictLinearToGrid = {25, 6};
    FunctionDocumentation::Category category_timeSeriesPredictLinearToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesPredictLinearToGrid = {description_timeSeriesPredictLinearToGrid, syntax_timeSeriesPredictLinearToGrid, arguments_timeSeriesPredictLinearToGrid, parameters_timeSeriesPredictLinearToGrid, returned_value_timeSeriesPredictLinearToGrid, examples_timeSeriesPredictLinearToGrid, introduced_in_timeSeriesPredictLinearToGrid, category_timeSeriesPredictLinearToGrid};

    factory.registerFunction("timeSeriesPredictLinearToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 5, "start_timestamp, end_timestamp, step, window, predict_offset");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                const Float64 predict_offset = extractTimeseriesFloatParameter(name, "predict_offset", parameters[4]) * static_cast<Float64>(DecimalUtils::scaleMultiplier<Int64>(scale));
                return std::make_shared<AggregateFunctionTimeseriesPredictLinearToGrid<TimestampType, IntervalType, ValueType>>(argument_types, parameters, start, end, step, window, scale, predict_offset);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesPredictLinearToGrid});

    /// timeSeriesLinearRegressionToGrid documentation
    FunctionDocumentation::Description description_timeSeriesLinearRegressionToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and fits a line to the values on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples within the specified time window are fitted by a line, and the function returns the tuple `(intercept, slope)`: `intercept` is the value of that line at the grid point's timestamp and `slope` is its slope per second. Both are NULL if there are not enough samples in the window.

The result of `timeSeriesPredictLinearToGrid` with the offset `t` equals `intercept + slope * t`, and the result of `timeSeriesDerivToGrid` equals `slope`. The PromQL function `predict_linear` is implemented with this function, which allows the prediction offset to differ between grid points, like in `predict_linear(v, time())`.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesLinearRegressionToGrid = R"(
timeSeriesLinearRegressionToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
timeSeriesLinearRegressionToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesLinearRegressionToGrid = {
        {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesLinearRegressionToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesLinearRegressionToGrid = {"The tuple `(intercept, slope)` for each grid point: the value of the fitted line at the grid point's timestamp and its slope per second. Both elements are NULL if there are not enough samples within the window for a particular grid point.", {"Array(Tuple(intercept Nullable(Float64), slope Nullable(Float64)))"}};
    FunctionDocumentation::Examples examples_timeSeriesLinearRegressionToGrid = {
    {
        "Fit a line on the grid [100, 110, 120] and compare with timeSeriesPredictLinearToGrid and timeSeriesDerivToGrid",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [100, 110, 120]::Array(DateTime) AS timestamps,
    [10, 20, 30]::Array(Float64) AS values,
    timeSeriesLinearRegressionToGrid(100, 120, 10, 30)(timestamps, values) AS regression
SELECT
    regression,
    arrayMap(r -> r.intercept + r.slope * 60, regression) AS predict_linear_60,
    arrayMap(r -> r.slope, regression) AS deriv;
        )",
        R"(
┌─regression──────────────────┬─predict_linear_60─┬─deriv──────┐
│ [(NULL,NULL),(20,1),(30,1)] │ [NULL,80,90]      │ [NULL,1,1] │
└─────────────────────────────┴───────────────────┴────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesLinearRegressionToGrid = {26, 9};
    FunctionDocumentation::Category category_timeSeriesLinearRegressionToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesLinearRegressionToGrid = {description_timeSeriesLinearRegressionToGrid, syntax_timeSeriesLinearRegressionToGrid, arguments_timeSeriesLinearRegressionToGrid, parameters_timeSeriesLinearRegressionToGrid, returned_value_timeSeriesLinearRegressionToGrid, examples_timeSeriesLinearRegressionToGrid, introduced_in_timeSeriesLinearRegressionToGrid, category_timeSeriesLinearRegressionToGrid};

    factory.registerFunction("timeSeriesLinearRegressionToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesLinearRegressionToGrid<TimestampType, IntervalType, ValueType>>(argument_types, parameters, start, end, step, window, scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesLinearRegressionToGrid});
}

}
