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
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like first_over_time](https://prometheus.io/docs/prometheus/latest/querying/functions/#first_over_time) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the earliest (smallest timestamp) sample within the specified time window is chosen.

<Warning>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
</Warning>
    )";
    FunctionDocumentation::Syntax syntax_timeSeriesFirstToGrid = R"(
timeSeriesFirstToGrid(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)
    )";
    FunctionDocumentation::Parameters parameters_timeSeriesFirstToGrid = {
        {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
        {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
        {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
        {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {"UInt32"}}
    };
    FunctionDocumentation::Arguments arguments_timeSeriesFirstToGrid = {
        {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
        {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSeriesFirstToGrid = {"Returns the earliest value within the window on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window for a particular grid point.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples_timeSeriesFirstToGrid = {
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
SELECT timeSeriesFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )",
        R"(
┌─timeSeriesFirstToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
│ [NULL,NULL,1,1,1,3,4,5,5]                                                               │
└─────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSeriesFirstToGrid = {26, 8};
    FunctionDocumentation::Category category_timeSeriesFirstToGrid = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_timeSeriesFirstToGrid = {description_timeSeriesFirstToGrid, syntax_timeSeriesFirstToGrid, arguments_timeSeriesFirstToGrid, parameters_timeSeriesFirstToGrid, returned_value_timeSeriesFirstToGrid, examples_timeSeriesFirstToGrid, introduced_in_timeSeriesFirstToGrid, category_timeSeriesFirstToGrid};

    factory.registerFunction("timeSeriesFirstToGrid",
        {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
        {
            assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
            auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
            {
                return std::make_shared<AggregateFunctionTimeseriesFirstToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
            };
            return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
        },
        documentation_timeSeriesFirstToGrid});
}

}
