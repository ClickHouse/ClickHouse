#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHistogramExtrapolatedValue.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHistogramInstantValue.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHistogramHelpers.h>

#include <fmt/core.h>

#include <algorithm>


namespace DB
{

/// The rate family over native histograms is registered in one place because all five functions share one documentation builder.
void registerAggregateFunctionTimeseriesHistogramRates(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeseriesHistogramRates(AggregateFunctionFactory & factory)
{
    /// The documentation shared by the five rate-family histogram aggregates; only the name, the PromQL function and
    /// the example's result grid differ (hand-computed through the upstream algorithm, verified bit-for-bit in 05031_timeseries_histogram_rate_aggregates).
    auto make_histogram_rate_documentation = [](const char * function_name, const char * promql_function, const char * promql_anchor, const char * value_name, const String & example_result)
    {
        FunctionDocumentation::Description description = fmt::format(R"(
Aggregate function that takes time series data as pairs of timestamps and native histograms and calculates [PromQL-like {}](https://prometheus.io/docs/prometheus/latest/querying/functions/#{}) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `{}` are considered within the specified time window.

The histogram argument is the payload tuple of the `histograms` target table of a `TimeSeries` table: `Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64))`.

The result at each grid point is the payload tuple of a synthetic gauge histogram, computed like Prometheus' `histogramRate`/`extrapolatedRate` (counter resets are detected and handled for `rate`/`increase`; `delta` treats the samples as gauges). The value is NULL if there are not enough samples within the window (fewer than two), or if the window mixes histograms with exponential and custom bucket schemas.

:::warning
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions=true`.
:::
    )", promql_function, promql_anchor, promql_function);
        FunctionDocumentation::Syntax syntax = fmt::format(R"(
{}(start_timestamp, end_timestamp, grid_step, staleness_window)(timestamp, histogram)
    )", function_name);
        FunctionDocumentation::Parameters parameters = {
            {"start_timestamp", "Specifies start of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
            {"end_timestamp", "Specifies end of the grid. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a date-time text.", {"UInt32", "DateTime", "DateTime64", "Float*", "Decimal*", "String"}},
            {"grid_step", "Specifies step of the grid in seconds. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}},
            {"staleness_window", "Specifies the maximum staleness of the considered samples in seconds. The staleness window is a left-open and right-closed interval. With a `DateTime64` timestamp argument it can also be a fractional number, or a string containing a number or a duration like '15s' or '1m'.", {"UInt32", "Float*", "Decimal*", "String"}}
        };
        FunctionDocumentation::Arguments arguments = {
            {"timestamp", "Timestamp of the sample.", {"UInt32", "DateTime", "DateTime64"}},
            {"histogram", "Native histogram sample corresponding to the timestamp: the payload tuple of the `histograms` target table.", {"Tuple"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {fmt::format("Returns {} values on the specified grid. The returned array contains one value for each time grid point: the payload tuple of the computed histogram. The value is NULL if there are not enough samples within the window.", value_name), {"Array(Nullable(Tuple))"}};

        /// Draw the Pretty-formatted single-column result of the example query like the engine would.
        const String header = fmt::format("{}(start_ts, end_ts, step_seconds, window_seconds)(timestamp, histogram)", function_name);
        const size_t width = std::max(header.size(), example_result.size());
        String dashes;
        for (size_t i = 0; i < width + 2; ++i)
            dashes += "─";
        const String example_output = fmt::format("\n┌─{}─┐\n│ {} │\n└{}┘\n",
            header + String(width - header.size(), ' '),
            example_result + String(width - example_result.size(), ' '),
            dashes);

        FunctionDocumentation::Examples examples = {
        {
            "Basic usage with individual timestamp-histogram pairs",
            fmt::format(R"(
SET enable_time_series_aggregate_functions = 1;
WITH
    [130, 140]::Array(UInt32) AS timestamps,
    [(0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [], 4, 0, [1, 3], []), (0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [], 8, 0, [2, 6], [])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64))) AS histograms,
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- window
SELECT {}(start_ts, end_ts, step_seconds, window_seconds)(timestamp, histogram)
FROM
(
    SELECT arrayJoin(arrayZip(timestamps, histograms)) AS sample,
        sample.1 AS timestamp,
        sample.2 AS histogram
);
        )", function_name),
            example_output
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
        return FunctionDocumentation{description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};
    };

    auto register_histogram_rate_function = [&](const char * function_name, auto aggregate_factory, FunctionDocumentation && documentation)
    {
        factory.registerFunction(function_name,
            {[=, factory_fn = std::move(aggregate_factory)](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings) -> AggregateFunctionPtr
            {
                assertTimeseriesParametersCount(name, parameters, 4, "start_timestamp, end_timestamp, step, window");
                auto make_function = [&]<typename TimestampType, typename IntervalType, typename ValueType>(TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
                {
                    return factory_fn.template operator()<TimestampType, IntervalType>(argument_types, parameters, start, end, step, window, scale);
                };
                return createAggregateFunctionTimeseriesHistogram(name, argument_types, parameters, settings, make_function);
            },
            std::move(documentation)});
    };

    register_histogram_rate_function("timeSeriesHistogramRateToGrid",
        []<typename TimestampType, typename IntervalType>(const DataTypes & argument_types, const Array & parameters, TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
        {
            return std::make_shared<AggregateFunctionTimeseriesHistogramRateToGrid<TimestampType, IntervalType>>(argument_types, parameters, start, end, step, window, scale);
        },
        make_histogram_rate_documentation("timeSeriesHistogramRateToGrid", "rate", "rate", "rate",
            "[NULL,NULL,NULL,NULL,(6,0,0,0.2222222222222222,0.611111111111111,0,[(0,2)],[0.05555555555555555,0.16666666666666666],[],[],[],0,0,[],[]),(6,0,0,0.2222222222222222,0.611111111111111,0,[(0,2)],[0.05555555555555555,0.16666666666666666],[],[],[],0,0,[],[]),NULL,NULL,NULL]"));

    register_histogram_rate_function("timeSeriesHistogramIncreaseToGrid",
        []<typename TimestampType, typename IntervalType>(const DataTypes & argument_types, const Array & parameters, TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
        {
            return std::make_shared<AggregateFunctionTimeseriesHistogramIncreaseToGrid<TimestampType, IntervalType>>(argument_types, parameters, start, end, step, window, scale);
        },
        make_histogram_rate_documentation("timeSeriesHistogramIncreaseToGrid", "increase", "increase", "increase",
            "[NULL,NULL,NULL,NULL,(6,0,0,10,27.5,0,[(0,2)],[2.5,7.5],[],[],[],0,0,[],[]),(6,0,0,10,27.5,0,[(0,2)],[2.5,7.5],[],[],[],0,0,[],[]),NULL,NULL,NULL]"));

    register_histogram_rate_function("timeSeriesHistogramDeltaToGrid",
        []<typename TimestampType, typename IntervalType>(const DataTypes & argument_types, const Array & parameters, TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
        {
            return std::make_shared<AggregateFunctionTimeseriesHistogramDeltaToGrid<TimestampType, IntervalType>>(argument_types, parameters, start, end, step, window, scale);
        },
        make_histogram_rate_documentation("timeSeriesHistogramDeltaToGrid", "delta", "delta", "delta",
            "[NULL,NULL,NULL,NULL,(6,0,0,10,27.5,0,[(0,2)],[2.5,7.5],[],[],[],0,0,[],[]),(6,0,0,10,27.5,0,[(0,2)],[2.5,7.5],[],[],[],0,0,[],[]),NULL,NULL,NULL]"));

    register_histogram_rate_function("timeSeriesHistogramInstantRateToGrid",
        []<typename TimestampType, typename IntervalType>(const DataTypes & argument_types, const Array & parameters, TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
        {
            return std::make_shared<AggregateFunctionTimeseriesHistogramInstantRateToGrid<TimestampType, IntervalType>>(argument_types, parameters, start, end, step, window, scale);
        },
        make_histogram_rate_documentation("timeSeriesHistogramInstantRateToGrid", "irate", "irate", "instant rate",
            "[NULL,NULL,NULL,NULL,(6,0,0,0.4,1.1,0,[(0,2)],[0.1,0.3],[],[],[],0,0,[],[]),(6,0,0,0.4,1.1,0,[(0,2)],[0.1,0.3],[],[],[],0,0,[],[]),NULL,NULL,NULL]"));

    register_histogram_rate_function("timeSeriesHistogramInstantDeltaToGrid",
        []<typename TimestampType, typename IntervalType>(const DataTypes & argument_types, const Array & parameters, TimestampType start, TimestampType end, IntervalType step, IntervalType window, UInt32 scale) -> AggregateFunctionPtr
        {
            return std::make_shared<AggregateFunctionTimeseriesHistogramInstantDeltaToGrid<TimestampType, IntervalType>>(argument_types, parameters, start, end, step, window, scale);
        },
        make_histogram_rate_documentation("timeSeriesHistogramInstantDeltaToGrid", "idelta", "idelta", "instant delta",
            "[NULL,NULL,NULL,NULL,(6,0,0,4,11,0,[(0,2)],[1,3],[],[],[],0,0,[],[]),(6,0,0,4,11,0,[(0,2)],[1,3],[],[],[],0,0,[],[]),NULL,NULL,NULL]"));
}

}
