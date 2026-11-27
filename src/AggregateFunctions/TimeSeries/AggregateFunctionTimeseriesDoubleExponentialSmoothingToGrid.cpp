#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates the [PromQL `double_exponential_smoothing`](https://prometheus.io/docs/prometheus/latest/querying/functions/#double_exponential_smoothing) function on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples within the specified time window are smoothed in the order of their timestamps, and the result is the last smoothed value.

The first smoothed value is the first sample, and the first trend is the difference of the first two samples. For each following sample `x`, the trend becomes `trend_factor * (level - previous_level) + (1 - trend_factor) * trend` and the smoothed value becomes `smoothing_factor * x + (1 - smoothing_factor) * (level + trend)`, where `level` and `previous_level` are the last two smoothed values.

The smoothing factor and the trend factor follow the samples as the last two arguments: each is either one number used at every grid point, or an array with one number per grid point. Each must be the same in every row. Both factors must be greater than 0 and less than 1. Like in Prometheus, they are checked only for a window that contains samples, and a NaN factor is not an error and gives NaN.

The samples can be passed in one of three forms:
- as two arguments `timestamp` and `value`, where each row holds a single sample;
- as two arrays of timestamps and values, where each row holds a whole time series;
- as a single array of `(timestamp, value)` tuples, where each row holds a whole time series.

If several samples have the same timestamp, only one of them is used: the sample with the greatest value. A NaN value loses to any other value, so a NaN value is used only if all samples at this timestamp are NaN.

<Note>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = R"(
timeSeriesDoubleExponentialSmoothingToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value, smoothing_factor, trend_factor)
timeSeriesDoubleExponentialSmoothingToGrid(start_timestamp, end_timestamp, grid_step, staleness)(samples, smoothing_factor, trend_factor)
    )";
    FunctionDocumentation::Parameters parameters_documentation = {
        {"start_timestamp", "Specifies start of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"end_timestamp", "Specifies end of the grid. It can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
        {"grid_step", "Specifies step of the grid in seconds. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
        {"staleness", "Specifies the maximum \"staleness\" in seconds of the considered samples. The staleness window is a left-open and right-closed interval. It can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
    };
    FunctionDocumentation::Arguments arguments = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"samples", "Samples of the time series passed as an array of tuples `(timestamp, value)`, where the tuple elements have the timestamp and value types listed above. An alternative to passing the timestamps and the values as two separate arguments.", {"Array(Tuple(T1, T2))"}},
        {"smoothing_factor", "Specifies how much a new sample changes the smoothed value: close to 0, the smoothed value follows the older samples; close to 1, the new sample. Either one number for the whole grid or an array with one number per grid point. Must be the same in every row.", {"Float*", "UInt8/16/32/64", "Int8/16/32/64", "Array(Float*)", "Array(UInt8/16/32/64)", "Array(Int8/16/32/64)"}},
        {"trend_factor", "Specifies how much the last change of the smoothed value changes the trend: close to 0, the trend follows the older trend; close to 1, the last change. Either one number for the whole grid or an array with one number per grid point. Must be the same in every row.", {"Float*", "UInt8/16/32/64", "Int8/16/32/64", "Array(Float*)", "Array(UInt8/16/32/64)", "Array(Int8/16/32/64)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the last smoothed value for each point of the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are fewer than two samples within the window for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Calculate double_exponential_smoothing values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210]",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    -- NOTE: the gap between 140 and 190 is to show that the grid points 180 and 195 have a single sample in the window
    [110, 120, 130, 140, 190, 200, 210]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8]::Array(Float64) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,          -- start of timestamp grid
    90 + 120 AS end_ts,      -- end of timestamp grid
    15 AS step_seconds,      -- step of timestamp grid
    45 AS window_seconds,    -- "staleness" window
    0.5 AS smoothing_factor,
    0.5 AS trend_factor
SELECT timeSeriesDoubleExponentialSmoothingToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values, smoothing_factor, trend_factor);
        )",
        R"(
┌─timeSeriesDoubleExponentialSmoothingToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values, smoothing_factor, trend_factor)─┐
│ [NULL,NULL,1,2,3.25,4,NULL,NULL,6.5]                                                                                                           │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters_documentation, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesDoubleExponentialSmoothingToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");

            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                /// The factors follow the samples: each is a number for the whole grid, or an array with a number for each grid point.
                /// Their range is not checked here but when a window with samples is calculated, like in PromQL.
                using Function = AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<TimestampType, ValueType>;
                checkTimeseriesScalarOrGridArgumentType(Function::smoothing_factor_names, argument_types[argument_types.size() - 2]);
                checkTimeseriesScalarOrGridArgumentType(Function::trend_factor_names, argument_types.back());

                return std::make_shared<Function>(
                    argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function,
                AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<DateTime64, Float64>::num_extra_arguments);
        },
        documentation});
}

}
