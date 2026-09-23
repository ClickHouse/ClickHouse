#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesTsOfToGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>

#include <fmt/format.h>

#include <vector>


namespace DB
{

void registerAggregateFunctionTimeseriesTsOfToGrid(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesTsOfToGrid(AggregateFunctionFactory & factory)
{
    /// timeSeriesTsOf{First,Last,Min,Max}ToGrid documentation and registration.
    /// These return the Unix timestamp (in seconds, as Float64) of a selected sample within the window:
    ///   - TsOfFirst / TsOfLast: the earliest / latest sample (keeps the metric name in PromQL).
    ///   - TsOfMin / TsOfMax:    the minimum- / maximum-value sample, latest one on ties (drops the metric name).
    struct TsOfRegistration
    {
        String function_name;
        String promql_name;
        String selected;                /// human description of which sample is selected
        String example_result;          /// expected result for the shared example dataset
    };

    const std::vector<TsOfRegistration> ts_of_registrations = { // STYLE_CHECK_ALLOW_STD_CONTAINERS
        {"timeSeriesTsOfFirstToGrid", "ts_of_first_over_time", "earliest (smallest timestamp) sample", "[NULL,NULL,110,110,110,130,140,190,190]"},
        {"timeSeriesTsOfLastToGrid",  "ts_of_last_over_time",  "latest (largest timestamp) sample",    "[NULL,NULL,120,130,140,140,140,190,210]"},
        {"timeSeriesTsOfMinToGrid",   "ts_of_min_over_time",   "minimum-value sample (latest one on ties)", "[NULL,NULL,120,120,120,130,140,190,200]"},
        {"timeSeriesTsOfMaxToGrid",   "ts_of_max_over_time",   "maximum-value sample (latest one on ties)", "[NULL,NULL,120,130,140,140,140,190,210]"},
    };

    for (const auto & reg : ts_of_registrations)
    {
        FunctionDocumentation::Description description = fmt::format(R"(
Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like {0}](https://prometheus.io/docs/prometheus/latest/querying/functions/#{0}) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid it returns the Unix timestamp (in seconds) of the {1} within the specified time window.

:::note
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )", reg.promql_name, reg.selected);

        FunctionDocumentation::Syntax syntax = fmt::format(
            "{}(start_timestamp, end_timestamp, grid_step, staleness)(timestamp, value)", reg.function_name);

        FunctionDocumentation::Parameters doc_parameters = {
            {"start_timestamp", "Specifies start of the grid.", {"UInt32", "DateTime"}},
            {"end_timestamp", "Specifies end of the grid.", {"UInt32", "DateTime"}},
            {"grid_step", "Specifies step of the grid in seconds.", {"UInt32"}},
            {"staleness", "Specifies the maximum staleness in seconds of the considered samples. The staleness window is a left-open and right-closed interval.", {"UInt32"}}
        };
        FunctionDocumentation::Arguments arguments = {
            {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "Array(UInt32)", "Array(DateTime)"}},
            {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float*", "Array(Float*)"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the Unix timestamp (in seconds) of the selected sample within the window on the specified grid. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window for a particular grid point.", {"Array(Nullable(Float64))"}};
        FunctionDocumentation::Examples examples = {
        {
            "Basic usage with individual timestamp-value pairs",
            fmt::format(R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT {0}(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
        )", reg.function_name),
            fmt::format("{}\n", reg.example_result)
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
        FunctionDocumentation documentation = {description, syntax, arguments, doc_parameters, returned_value, examples, introduced_in, category};

        if (reg.function_name == "timeSeriesTsOfFirstToGrid")
            factory.registerFunction("timeSeriesTsOfFirstToGrid",
                {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
                {
                    assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
                    auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionTimeseriesTsOfFirstToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
                    };
                    return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
                },
                documentation});
        else if (reg.function_name == "timeSeriesTsOfLastToGrid")
            factory.registerFunction("timeSeriesTsOfLastToGrid",
                {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
                {
                    assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
                    auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionTimeseriesTsOfLastToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
                    };
                    return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
                },
                documentation});
        else if (reg.function_name == "timeSeriesTsOfMinToGrid")
            factory.registerFunction("timeSeriesTsOfMinToGrid",
                {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
                {
                    assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
                    auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionTimeseriesTsOfMinToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
                    };
                    return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
                },
                documentation});
        else
            factory.registerFunction("timeSeriesTsOfMaxToGrid",
                {[](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
                {
                    assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
                    auto make_function = [&]<typename TimestampType, typename ValueType>(DateTime64 start, DateTime64 end, Decimal64 step, Decimal64 window, UInt32 grid_scale, UInt32 column_timestamp_scale) -> AggregateFunctionPtr
                    {
                        return std::make_shared<AggregateFunctionTimeseriesTsOfMaxToGrid<TimestampType, ValueType>>(argument_types, parameters, start, end, step, window, grid_scale, column_timestamp_scale);
                    };
                    return createAggregateFunctionTimeseries(name, argument_types, parameters, settings, make_function);
                },
                documentation});
    }
}

}
