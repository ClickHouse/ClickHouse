#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesCreation.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTime.h>
#include <Common/FunctionDocumentation.h>


namespace DB
{

namespace
{

/// PromQL range-vector aggregators (`avg_over_time`, `count_over_time`, …) are documented together; use the functions reference anchor `#aggregation_over_time` (the page section is titled `_over_time()`). Per-function fragments such as `#count_over_time` are not valid on that page.
const FunctionDocumentation::Arguments arguments_timeseries_over_time_grid = {
    {"timestamp", "Timestamp of the sample. Can be individual values or arrays.", {"UInt32", "DateTime", "DateTime64", "Array(UInt32)", "Array(DateTime)", "Array(DateTime64)"}},
    {"value", "Value of the time series corresponding to the timestamp. Can be individual values or arrays.", {"Float32", "Float64", "Array(Float32)", "Array(Float64)"}}
};

const FunctionDocumentation::Parameters parameters_timeseries_over_time_grid_4 = {
    {"start_timestamp", "First grid timestamp (inclusive). Must use the same physical time unit as the `timestamp` argument (`DateTime64` parameters are normalized to the column scale).", {"UInt32", "DateTime", "DateTime64"}},
    {"end_timestamp", "Last grid timestamp (inclusive). The result array has one element per grid point from `start_timestamp` through `end_timestamp` in steps of `grid_step`.", {"UInt32", "DateTime", "DateTime64"}},
    {"grid_step", "Distance between consecutive grid timestamps. Seconds for `UInt32` / `DateTime`; for `DateTime64`, use the same subsecond step unit as the timestamp column.", {"UInt32", "DateTime", "DateTime64"}},
    {"window", "Sliding lookback length in the same time unit as the grid. At grid time `t`, a sample at `s` is in range iff `s > t - window` and `s <= t` (Prometheus-style half-open interval, analogous to `*_over_time` range vectors).", {"UInt32", "DateTime", "DateTime64"}}
};

const FunctionDocumentation::Parameters parameters_timeseries_over_time_grid_quantile = {
    {"start_timestamp", "First grid timestamp (inclusive). Must use the same physical time unit as the `timestamp` argument (`DateTime64` parameters are normalized to the column scale).", {"UInt32", "DateTime", "DateTime64"}},
    {"end_timestamp", "Last grid timestamp (inclusive). The result array has one element per grid point from `start_timestamp` through `end_timestamp` in steps of `grid_step`.", {"UInt32", "DateTime", "DateTime64"}},
    {"grid_step", "Distance between consecutive grid timestamps. Seconds for `UInt32` / `DateTime`; for `DateTime64`, use the same subsecond step unit as the timestamp column.", {"UInt32", "DateTime", "DateTime64"}},
    {"window", "Sliding lookback length in the same time unit as the grid. At grid time `t`, a sample at `s` is in range iff `s > t - window` and `s <= t`.", {"UInt32", "DateTime", "DateTime64"}},
    {"phi", "Quantile level in `[0, 1]`, matching Prometheus `quantile_over_time`. This value is dimensionless and must not be scaled with the timestamp unit.", {"Float64", "Float32", "UInt64", "Int64"}}
};

const FunctionDocumentation::ReturnedValue returned_value_timeseries_over_time_grid_float = {
    "Returns `Array(Nullable(T))` with one entry per grid timestamp (same order as `range(start_timestamp, end_timestamp + 1, grid_step)`). NULL means no sample fell in the active window at that grid point (or the aggregate is undefined). Non-NULL values use the same floating type as the input values.",
    {"Array(Nullable(Float32))", "Array(Nullable(Float64))"}
};

const FunctionDocumentation::IntroducedIn introduced_in_timeseries_over_time_grid{26, 5};
const FunctionDocumentation::Category category_timeseries = FunctionDocumentation::Category::AggregateFunction;

const String experimental_warning_timeseries_over_time_grid = R"(

:::warning
This function is experimental. Enable it with `allow_experimental_time_series_aggregate_functions` or `allow_experimental_time_series_table` (the legacy name `allow_experimental_ts_to_grid_aggregate_function` is an alias of the first setting).
:::
)";

const String description_suffix_timeseries_over_time_grid = R"(

At each grid timestamp `t`, a sample at `s` participates in the aggregate iff `s > t - window` and `s <= t` (Prometheus-style half-open lookback, matching `*_over_time` range vectors). Grid parameters `start_timestamp`, `end_timestamp`, `grid_step`, and `window` use the same physical unit as the `timestamp` column; for `DateTime64`, they are normalized to the column scale.
)";

const String & overTimeDocsExamplePrefix()
{
    static const String prefix = R"(SET allow_experimental_time_series_aggregate_functions = 1;

WITH
    [1734955421, 1734955436, 1734955451, 1734955466, 1734955481, 1734955496, 1734955511, 1734955526, 1734955541, 1734955556, 1734955571, 1734955586, 1734955601, 1734955616, 1734955631, 1734955646, 1734955661, 1734955676]::Array(UInt32) AS timestamps,
    [0, 0, 1, 1, 1, 3, 3, 3, 5, 3, 3, 3, 2, 4, 6, 8, 8, 8]::Array(Float64) AS values,
    1734955380 AS start, 1734955680 AS end, 60 AS step, 120 AS window,
    range(start, end + 1, step) AS grid
)";
    return prefix;
}

String makeOverTimeDocsExampleQuery(const String & aggregate_expr, const String & result_alias)
{
    return overTimeDocsExamplePrefix() + "SELECT arrayZip(grid, " + aggregate_expr + ") AS " + result_alias + "\nFORMAT Vertical;\n";
}

FunctionDocumentation::Examples singleOverTimeExample(
    const String & example_name, const String & aggregate_expr, const String & result_alias, const String & result_text)
{
    return {{example_name, makeOverTimeDocsExampleQuery(aggregate_expr, result_alias), result_text}};
}

/// Matches `tests/queries/0_stateless/04070_over_time_timeseries_functions.reference` (UInt32 unix timestamps in WITH).
FunctionDocumentation::Examples quantileOverTimeDocsExamples()
{
    return {
        {"quantile50_over_time_to_grid",
            makeOverTimeDocsExampleQuery(
                "timeSeriesQuantileOverTimeToGrid(start, end, step, window, 0.5)(timestamps, values)",
                "quantile50_2m"),
            R"(
Row 1:
──────
quantile50_2m: [(1734955380,NULL),(1734955440,0),(1734955500,1),(1734955560,3),(1734955620,3),(1734955680,5)]
)"},
        {"quantile90_over_time_to_grid",
            makeOverTimeDocsExampleQuery(
                "timeSeriesQuantileOverTimeToGrid(start, end, step, window, 0.9)(timestamps, values)",
                "quantile90_2m"),
            R"(
Row 1:
──────
quantile90_2m: [(1734955380,NULL),(1734955440,0),(1734955500,2),(1734955560,3.5999999999999996),(1734955620,4.3),(1734955680,8)]
)"},
    };
}

FunctionDocumentation makeOverTimeGridDocs(
    FunctionDocumentation::Description main_description,
    FunctionDocumentation::Syntax syntax,
    const FunctionDocumentation::Parameters & parameters,
    FunctionDocumentation::Examples examples,
    FunctionDocumentation::ReturnedValue returned_value = returned_value_timeseries_over_time_grid_float)
{
    return {
        FunctionDocumentation::Description(String(std::move(main_description)) + description_suffix_timeseries_over_time_grid + experimental_warning_timeseries_over_time_grid),
        std::move(syntax),
        arguments_timeseries_over_time_grid,
        parameters,
        std::move(returned_value),
        std::move(examples),
        introduced_in_timeseries_over_time_grid,
        category_timeseries};
}

}

void registerAggregateFunctionTimeseriesOverTimeGrid(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesAvgOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesAvgOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `avg_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid: for each grid point, averages sample values in the sliding `window` ending at that point.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesAvgOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "avg_over_time_to_grid",
                    "timeSeriesAvgOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "avg_2m",
                    R"(
Row 1:
──────
avg_2m:   [(1734955380,NULL),(1734955440,0),(1734955500,1),(1734955560,2.5),(1734955620,3.25),(1734955680,5.25)]
)"))});

    factory.registerFunction("timeSeriesMinOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesMinOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `min_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesMinOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "min_over_time_to_grid",
                    "timeSeriesMinOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "min_2m",
                    R"(
Row 1:
──────
min_2m:   [(1734955380,NULL),(1734955440,0),(1734955500,0),(1734955560,1),(1734955620,2),(1734955680,2)]
)"))});

    factory.registerFunction("timeSeriesMaxOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesMaxOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `max_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesMaxOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "max_over_time_to_grid",
                    "timeSeriesMaxOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "max_2m",
                    R"(
Row 1:
──────
max_2m:   [(1734955380,NULL),(1734955440,0),(1734955500,3),(1734955560,5),(1734955620,5),(1734955680,8)]
)"))});

    factory.registerFunction("timeSeriesSumOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesSumOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `sum_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesSumOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "sum_over_time_to_grid",
                    "timeSeriesSumOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "sum_2m",
                    R"(
Row 1:
──────
sum_2m:   [(1734955380,NULL),(1734955440,0),(1734955500,6),(1734955560,20),(1734955620,26),(1734955680,42)]
)"))});

    factory.registerFunction("timeSeriesCountOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesCountOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `count_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid: number of samples in the window.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesCountOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "count_over_time_to_grid",
                    "timeSeriesCountOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "count_2m",
                    R"(
Row 1:
──────
count_2m: [(1734955380,NULL),(1734955440,2),(1734955500,6),(1734955560,8),(1734955620,8),(1734955680,8)]
)"))});

    factory.registerFunction("timeSeriesStddevOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesStddevPopOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes population standard deviation over samples in the window, analogous to [PromQL `stddev_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) (population form).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesStddevOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "stddev_over_time_to_grid",
                    "timeSeriesStddevOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "stddev_2m",
                    R"(
Row 1:
──────
stddev_2m:  [(1734955380,NULL),(1734955440,0),(1734955500,1),(1734955560,1.3228756555322954),(1734955620,0.82915619758885),(1734955680,2.384848003542364)]
)"))});

    factory.registerFunction("timeSeriesStdvarOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesStdvarPopOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes population variance over samples in the window, analogous to [PromQL `stdvar_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) (population form).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesStdvarOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "stdvar_over_time_to_grid",
                    "timeSeriesStdvarOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "stdvar_2m",
                    R"(
Row 1:
──────
stdvar_2m:  [(1734955380,NULL),(1734955440,0),(1734955500,1),(1734955560,1.75),(1734955620,0.6875),(1734955680,5.6875)]
)"))});

    factory.registerFunction("timeSeriesPresentOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesPresentOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `present_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid: non-zero when any sample exists in the window.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesPresentOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "present_over_time_to_grid",
                    "timeSeriesPresentOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "present_2m",
                    R"(
Row 1:
──────
present_2m: [(1734955380,NULL),(1734955440,1),(1734955500,1),(1734955560,1),(1734955620,1),(1734955680,1)]
)"))});

    factory.registerFunction("timeSeriesAbsentOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesAbsentOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `absent_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid: non-zero when no sample exists in the window.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesAbsentOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "absent_over_time_to_grid",
                    "timeSeriesAbsentOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "absent_2m",
                    R"(
Row 1:
──────
absent_2m:  [(1734955380,1),(1734955440,NULL),(1734955500,NULL),(1734955560,NULL),(1734955620,NULL),(1734955680,NULL)]
)"))});

    factory.registerFunction("timeSeriesFirstOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesFirstOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Returns the value of the earliest sample (by timestamp) in the sliding window at each grid point, analogous to [PromQL `first_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesFirstOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "first_over_time_to_grid",
                    "timeSeriesFirstOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "first_2m",
                    R"(
Row 1:
──────
first_2m:       [(1734955380,NULL),(1734955440,0),(1734955500,0),(1734955560,1),(1734955620,3),(1734955680,3)]
)"))});

    factory.registerFunction("timeSeriesTsOfLastOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesTsOfLastOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
At each grid point, returns the timestamp of the latest sample in the window as seconds (see [Prometheus aggregation over time](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) for related PromQL such as `last_over_time`), encoded in the result value type.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfLastOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "ts_of_last_over_time_to_grid",
                    "timeSeriesTsOfLastOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "ts_of_last_2m",
                    R"(
Row 1:
──────
ts_of_last_2m:  [(1734955380,NULL),(1734955440,1734955436),(1734955500,1734955496),(1734955560,1734955556),(1734955620,1734955616),(1734955680,1734955676)]
)"))});

    factory.registerFunction("timeSeriesTsOfFirstOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesTsOfFirstOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
At each grid point, returns the timestamp of the earliest sample in the window as seconds, encoded in the result value type.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfFirstOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "ts_of_first_over_time_to_grid",
                    "timeSeriesTsOfFirstOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "ts_of_first_2m",
                    R"(
Row 1:
──────
ts_of_first_2m: [(1734955380,NULL),(1734955440,1734955421),(1734955500,1734955421),(1734955560,1734955451),(1734955620,1734955511),(1734955680,1734955571)]
)"))});

    factory.registerFunction("timeSeriesTsOfMinOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesTsOfMinOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
At each grid point, returns the timestamp (as seconds) of the sample whose value is the minimum over the window; ties are resolved by the aggregate implementation.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfMinOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "ts_of_min_over_time_to_grid",
                    "timeSeriesTsOfMinOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "ts_of_min_2m",
                    R"(
Row 1:
──────
ts_of_min_2m: [(1734955380,NULL),(1734955440,1734955421),(1734955500,1734955421),(1734955560,1734955451),(1734955620,1734955601),(1734955680,1734955601)]
)"))});

    factory.registerFunction("timeSeriesTsOfMaxOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesTsOfMaxOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
At each grid point, returns the timestamp (as seconds) of the sample whose value is the maximum over the window; ties are resolved by the aggregate implementation.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfMaxOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "ts_of_max_over_time_to_grid",
                    "timeSeriesTsOfMaxOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "ts_of_max_2m",
                    R"(
Row 1:
──────
ts_of_max_2m: [(1734955380,NULL),(1734955440,1734955421),(1734955500,1734955496),(1734955560,1734955541),(1734955620,1734955541),(1734955680,1734955646)]
)"))});

    factory.registerFunction("timeSeriesQuantileOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, true, AggregateFunctionTimeseriesQuantileOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `quantile_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid, using the same empirical quantile interpolation as Prometheus (`phi` in `[0, 1]`).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesQuantileOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window, phi)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_quantile,
                quantileOverTimeDocsExamples())});

    factory.registerFunction("timeSeriesMadOverTimeToGrid",
        {createAggregateFunctionTimeseries<false, false, false, AggregateFunctionTimeseriesMadOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes median absolute deviation over the window, analogous to [PromQL `mad_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesMadOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_4,
                singleOverTimeExample(
                    "mad_over_time_to_grid",
                    "timeSeriesMadOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "mad_2m",
                    R"(
Row 1:
──────
mad_2m: [(1734955380,NULL),(1734955440,0),(1734955500,0.5),(1734955560,1),(1734955620,0),(1734955680,2.5)]
)"))});
}

}
