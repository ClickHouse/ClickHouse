#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesMadToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>


namespace DB
{

void registerAggregateFunctionTimeseriesMadToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesMadToGrid(AggregateFunctionFactory & factory)
{
    /// timeSeriesMadToGrid documentation
    FunctionDocumentation::Description description_timeSeriesMadToGrid = R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like mad_over_time](https://prometheus.io/docs/prometheus/latest/querying/functions/#mad_over_time) (median absolute deviation) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples within the specified time window are considered.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesMadToGrid = R"(
timeSeriesMadToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesMadToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesMadToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesMadToGrid = {"Returns the median absolute deviation within the window on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesMadToGrid = {
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
    45 AS window_seconds
SELECT timeSeriesMadToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesMadToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,0,0,1,0.5,0,0,0]                                                           │
└───────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesMadToGrid = {26, 8};
    FunctionDocumentation::Category category_timeSeriesMadToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesMadToGrid = {description_timeSeriesMadToGrid, syntax_timeSeriesMadToGrid, arguments_timeSeriesMadToGrid, parameters_timeSeriesMadToGrid, returned_value_timeSeriesMadToGrid, examples_timeSeriesMadToGrid, introduced_in_timeSeriesMadToGrid, category_timeSeriesMadToGrid};

    factory.registerFunction("timeSeriesMadToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesMadToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesMadToGrid});
}

}
