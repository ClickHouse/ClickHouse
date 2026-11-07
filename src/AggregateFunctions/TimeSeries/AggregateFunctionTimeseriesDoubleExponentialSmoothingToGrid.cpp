#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

void registerAggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid(AggregateFunctionFactory & factory)
{
    /// timeSeriesDoubleExponentialSmoothingToGrid documentation
    FunctionDocumentation::Description description_timeSeriesDoubleExponentialSmoothingToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like double_exponential_smoothing](https://prometheus.io/docs/prometheus/latest/querying/functions/#double_exponential_smoothing) (Holt-Winters double exponential smoothing) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples within the specified time window are smoothed using the smoothing factor and trend factor, and the last smoothed value is returned.

The smoothing factor and the trend factor follow the samples as the last two arguments: each is either one number used at every grid point, or an array with one number per grid point. Each must be the same in every row. Like in Prometheus, a factor outside the open interval (0, 1) is an error for a grid point whose window has samples, and a NaN factor is not an error.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesDoubleExponentialSmoothingToGrid = R"(
timeSeriesDoubleExponentialSmoothingToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value, smoothing_factor, trend_factor)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesDoubleExponentialSmoothingToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesDoubleExponentialSmoothingToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}},
        {"smoothing_factor", "Smoothing factor for the level, in the open interval (0, 1): either one number for the whole grid or an array with one number per grid point. Must be the same in every row.", {"Float*", "UInt8/16/32/64", "Int8/16/32/64", "Array(Float*)", "Array(UInt8/16/32/64)", "Array(Int8/16/32/64)"}},
        {"trend_factor", "Smoothing factor for the trend, in the open interval (0, 1): either one number for the whole grid or an array with one number per grid point. Must be the same in every row.", {"Float*", "UInt8/16/32/64", "Int8/16/32/64", "Array(Float*)", "Array(UInt8/16/32/64)", "Array(Int8/16/32/64)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesDoubleExponentialSmoothingToGrid = {"Returns the smoothed value within the window on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are fewer than two samples within the window for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesDoubleExponentialSmoothingToGrid = {
    {
        "Basic usage with individual timestamp-value pairs",
        R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds,
    0.5 AS smoothing_factor,
    0.5 AS trend_factor
SELECT timeSeriesDoubleExponentialSmoothingToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value, smoothing_factor, trend_factor)
FROM
(
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesDoubleExponentialSmoothingToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value, smoothing_factor, trend_factor)─┐
│ [NULL,NULL,1,2,3.25,4,NULL,NULL,6.5]                                                                                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesDoubleExponentialSmoothingToGrid = {26, 10};
    FunctionDocumentation::Category category_timeSeriesDoubleExponentialSmoothingToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesDoubleExponentialSmoothingToGrid = {description_timeSeriesDoubleExponentialSmoothingToGrid, syntax_timeSeriesDoubleExponentialSmoothingToGrid, arguments_timeSeriesDoubleExponentialSmoothingToGrid, parameters_timeSeriesDoubleExponentialSmoothingToGrid, returned_value_timeSeriesDoubleExponentialSmoothingToGrid, examples_timeSeriesDoubleExponentialSmoothingToGrid, introduced_in_timeSeriesDoubleExponentialSmoothingToGrid, category_timeSeriesDoubleExponentialSmoothingToGrid};

    factory.registerFunction("timeSeriesDoubleExponentialSmoothingToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                /// The two factors follow the samples: each is a number for the whole grid, or an array with a number for each grid point.
                for (size_t i = argument_types.size() - 2; i < argument_types.size(); ++i)
                {
                    const auto * array_type = typeid_cast<const DataTypeArray *>(argument_types[i].get());
                    if (!isNativeNumber(array_type ? array_type->getNestedType() : argument_types[i]))
                        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument {} for aggregate function {}, expected a number or an array of numbers",
                            argument_types[i]->getName(), i + 1, name);
                }
                return std::make_shared<AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function,
                AggregateFunctionTimeseriesDoubleExponentialSmoothingToGrid<DateTime64, Float64>::num_extra_arguments);
        },
        documentation_timeSeriesDoubleExponentialSmoothingToGrid});
}

}
