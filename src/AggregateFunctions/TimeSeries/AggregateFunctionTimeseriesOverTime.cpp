#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesCreation.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTime.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesOverTimeStatsAligned.h>
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

/// Documentation parameters for the base `_over_time` aggregates: 4 grid parameters
/// (start_timestamp, end_timestamp, grid_step, window) and no extra per-function parameter.
/// `quantile_over_time` uses `parameters_timeseries_over_time_grid_quantile` below instead,
/// which adds a 5th `phi` parameter.
const FunctionDocumentation::Parameters parameters_timeseries_over_time_grid_base = {
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesAvgOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `avg_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid: for each grid point, averages sample values in the sliding `window` ending at that point.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesAvgOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesMinOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `min_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesMinOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesMaxOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `max_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesMaxOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesSumOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `sum_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesSumOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesCountOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `count_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid: number of samples in the window.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesCountOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "count_over_time_to_grid",
                    "timeSeriesCountOverTimeToGrid(start, end, step, window)(timestamps, values)",
                    "count_2m",
                    R"(
Row 1:
──────
count_2m: [(1734955380,NULL),(1734955440,2),(1734955500,6),(1734955560,8),(1734955620,8),(1734955680,8)]
)"))});

    /// Experimental stats-bucket variants — same per-grid semantics as the matching non-suffix
    /// function on aligned windows (`window % grid_step == 0 && window >= grid_step`), but
    /// `O(grid)` finalize instead of `O(grid × samples_per_window)`. Throws `BAD_ARGUMENTS`
    /// for unaligned windows; use the non-suffixed function for those. In `prometheusQuery()`,
    /// routing to these variants is controlled by `prometheus_query_use_stats_bucket` (off by default).
    factory.registerFunction("timeSeriesCountOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesCountOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAligned>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesCountOverTimeToGrid`, but uses a stats-bucket aligned fast path: each per-bucket count is folded into a running sum slid across the grid in `O(grid)` instead of revisiting every sample at every grid point.

Requires `window` to be a positive integer multiple of `grid_step` and `window >= grid_step`. Use `timeSeriesCountOverTimeToGrid` (no `_stats` suffix) for unaligned windows; unaligned arguments here raise `BAD_ARGUMENTS`.

In `prometheusQuery()`, set `prometheus_query_use_stats_bucket = 1` to route aligned `*_over_time` queries to `_stats` variants; the default (`0`) always uses the baseline aggregates. These are separate aggregate functions from their baseline counterparts — do not merge partial states across function names.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesCountOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "count_over_time_to_grid_stats",
                    "timeSeriesCountOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "count_2m_stats",
                    R"(
Row 1:
──────
count_2m_stats: [(1734955380,NULL),(1734955440,2),(1734955500,6),(1734955560,8),(1734955620,8),(1734955680,8)]
)"))});

    factory.registerFunction("timeSeriesSumOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesSumOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAligned>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesSumOverTimeToGrid`, but uses the stats-bucket aligned fast path. See `timeSeriesCountOverTimeToGrid_stats` for the alignment contract and PromQL routing notes.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesSumOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "sum_over_time_to_grid_stats",
                    "timeSeriesSumOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "sum_2m_stats",
                    R"(
Row 1:
──────
sum_2m_stats:   [(1734955380,NULL),(1734955440,0),(1734955500,6),(1734955560,20),(1734955620,26),(1734955680,42)]
)"))});

    factory.registerFunction("timeSeriesAvgOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesAvgOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAligned>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesAvgOverTimeToGrid`, but uses the stats-bucket aligned fast path. See `timeSeriesCountOverTimeToGrid_stats` for the alignment contract and PromQL routing notes.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesAvgOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "avg_over_time_to_grid_stats",
                    "timeSeriesAvgOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "avg_2m_stats",
                    R"(
Row 1:
──────
avg_2m_stats:   [(1734955380,NULL),(1734955440,0),(1734955500,1),(1734955560,2.5),(1734955620,3.25),(1734955680,5.25)]
)"))});

    factory.registerFunction("timeSeriesStddevOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesStddevOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAligned>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesStddevOverTimeToGrid`, but uses the stats-bucket aligned fast path. Uses the same population-stddev formula `sqrt(E[x²] - E[x]²)` as the baseline; numerical results are identical up to floating-point rounding because `(count, sum, sum_sq)` are accumulated additively at the bucket boundary.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesStddevOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "stddev_over_time_to_grid_stats",
                    "timeSeriesStddevOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "stddev_2m_stats",
                    R"(
Row 1:
──────
stddev_2m_stats:  [(1734955380,NULL),(1734955440,0),(1734955500,1),(1734955560,1.3228756555322954),(1734955620,0.82915619758885),(1734955680,2.384848003542364)]
)"))});

    factory.registerFunction("timeSeriesStdvarOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesStdvarOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAligned>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesStdvarOverTimeToGrid`, but uses the stats-bucket aligned fast path. Uses the same population-variance formula `E[x²] - E[x]²` as the baseline.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesStdvarOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "stdvar_over_time_to_grid_stats",
                    "timeSeriesStdvarOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "stdvar_2m_stats",
                    R"(
Row 1:
──────
stdvar_2m_stats:  [(1734955380,NULL),(1734955440,0),(1734955500,1),(1734955560,1.75),(1734955620,0.6875),(1734955680,5.6875)]
)"))});

    factory.registerFunction("timeSeriesPresentOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesPresentOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAligned>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesPresentOverTimeToGrid`, but uses the stats-bucket aligned fast path with a presence-only bucket: each bucket stores a single `bool`, and the driver tracks how many active buckets sit in the window.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesPresentOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "present_over_time_to_grid_stats",
                    "timeSeriesPresentOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "present_2m_stats",
                    R"(
Row 1:
──────
present_2m_stats: [(1734955380,NULL),(1734955440,1),(1734955500,1),(1734955560,1),(1734955620,1),(1734955680,1)]
)"))});

    factory.registerFunction("timeSeriesAbsentOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesAbsentOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAligned>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesAbsentOverTimeToGrid`, but uses the stats-bucket aligned fast path.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesAbsentOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "absent_over_time_to_grid_stats",
                    "timeSeriesAbsentOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "absent_2m_stats",
                    R"(
Row 1:
──────
absent_2m_stats:  [(1734955380,1),(1734955440,NULL),(1734955500,NULL),(1734955560,NULL),(1734955620,NULL),(1734955680,NULL)]
)"))});

    /// --- min / max (mono-deque driver) ---
    factory.registerFunction("timeSeriesMinOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesMinOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesMinOverTimeToGrid`, but uses the stats-bucket aligned fast path with a monotonic-deque driver over per-bucket `min_val`.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesMinOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "min_over_time_to_grid_stats",
                    "timeSeriesMinOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "min_2m_stats",
                    R"(
Row 1:
──────
min_2m_stats:   [(1734955380,NULL),(1734955440,0),(1734955500,0),(1734955560,1),(1734955620,2),(1734955680,2)]
)"))});

    factory.registerFunction("timeSeriesMaxOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesMaxOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesMaxOverTimeToGrid`, but uses the stats-bucket aligned fast path with a monotonic-deque driver over per-bucket `max_val`.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesMaxOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "max_over_time_to_grid_stats",
                    "timeSeriesMaxOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "max_2m_stats",
                    R"(
Row 1:
──────
max_2m_stats:   [(1734955380,NULL),(1734955440,0),(1734955500,3),(1734955560,5),(1734955620,5),(1734955680,8)]
)"))});

    /// --- first / last / ts_of_first / ts_of_last (two-pointer driver) ---
    factory.registerFunction("timeSeriesFirstOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesFirstOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerLeft>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesFirstOverTimeToGrid`, but uses the stats-bucket aligned fast path with a two-pointer driver over the leftmost active bucket per grid step.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesFirstOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "first_over_time_to_grid_stats",
                    "timeSeriesFirstOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "first_2m_stats",
                    R"(
Row 1:
──────
first_2m_stats:       [(1734955380,NULL),(1734955440,0),(1734955500,0),(1734955560,1),(1734955620,3),(1734955680,3)]
)"))});

    factory.registerFunction("timeSeriesTsOfFirstOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesTsOfFirstOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerLeft>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesTsOfFirstOverTimeToGrid`, but uses the stats-bucket aligned fast path. Returns the timestamp of the earliest sample in the active window.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfFirstOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "ts_of_first_over_time_to_grid_stats",
                    "timeSeriesTsOfFirstOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "ts_of_first_2m_stats",
                    R"(
Row 1:
──────
ts_of_first_2m_stats: [(1734955380,NULL),(1734955440,1734955421),(1734955500,1734955421),(1734955560,1734955451),(1734955620,1734955511),(1734955680,1734955571)]
)"))});

    factory.registerFunction("timeSeriesLastOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesLastOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerRight>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Returns the value of the latest sample in the active window — analogous to PromQL `last_over_time`. Uses the stats-bucket aligned fast path with a two-pointer driver over the rightmost active bucket per grid step.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesLastOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "last_over_time_to_grid_stats",
                    "timeSeriesLastOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "last_2m_stats",
                    R"(
Row 1:
──────
last_2m_stats:        [(1734955380,NULL),(1734955440,0),(1734955500,3),(1734955560,3),(1734955620,3),(1734955680,8)]
)"))});

    factory.registerFunction("timeSeriesTsOfLastOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesTsOfLastOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAlignedTwoPointerRight>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesTsOfLastOverTimeToGrid`, but uses the stats-bucket aligned fast path.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfLastOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "ts_of_last_over_time_to_grid_stats",
                    "timeSeriesTsOfLastOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "ts_of_last_2m_stats",
                    R"(
Row 1:
──────
ts_of_last_2m_stats:  [(1734955380,NULL),(1734955440,1734955436),(1734955500,1734955496),(1734955560,1734955556),(1734955620,1734955616),(1734955680,1734955676)]
)"))});

    /// --- ts_of_min / ts_of_max (mono-deque driver with VM rollupT* tie-break) ---
    factory.registerFunction("timeSeriesTsOfMinOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesTsOfMinOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesTsOfMinOverTimeToGrid` (matches the VictoriaMetrics `rollupTmin` last-ts tie-break semantics), but uses the stats-bucket aligned fast path.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfMinOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "ts_of_min_over_time_to_grid_stats",
                    "timeSeriesTsOfMinOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "ts_of_min_2m_stats",
                    R"(
Row 1:
──────
ts_of_min_2m_stats: [(1734955380,NULL),(1734955440,1734955421),(1734955500,1734955421),(1734955560,1734955451),(1734955620,1734955601),(1734955680,1734955601)]
)"))});

    factory.registerFunction("timeSeriesTsOfMaxOverTimeToGrid_stats",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesTsOfMaxOverTimeStatsAlignedTraits, AggregateFunctionTimeseriesOverTimeStatsAlignedMonoDeque>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Same as `timeSeriesTsOfMaxOverTimeToGrid` (matches the VictoriaMetrics `rollupTmax` last-ts tie-break semantics), but uses the stats-bucket aligned fast path.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfMaxOverTimeToGrid_stats(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
                singleOverTimeExample(
                    "ts_of_max_over_time_to_grid_stats",
                    "timeSeriesTsOfMaxOverTimeToGrid_stats(start, end, step, window)(timestamps, values)",
                    "ts_of_max_2m_stats",
                    R"(
Row 1:
──────
ts_of_max_2m_stats: [(1734955380,NULL),(1734955440,1734955421),(1734955500,1734955496),(1734955560,1734955541),(1734955620,1734955541),(1734955680,1734955646)]
)"))});

    factory.registerFunction("timeSeriesStddevOverTimeToGrid",
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesStddevPopOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes population standard deviation over samples in the window, analogous to [PromQL `stddev_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) (population form).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesStddevOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesStdvarPopOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes population variance over samples in the window, analogous to [PromQL `stdvar_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) (population form).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesStdvarOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesPresentOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `present_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid: non-zero when any sample exists in the window.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesPresentOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesAbsentOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes a [PromQL-like `absent_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) on a regular time grid: non-zero when no sample exists in the window.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesAbsentOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesFirstOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Returns the value of the earliest sample (by timestamp) in the sliding window at each grid point, analogous to [PromQL `first_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesFirstOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesTsOfLastOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
At each grid point, returns the timestamp of the latest sample in the window as seconds (see [Prometheus aggregation over time](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time) for related PromQL such as `last_over_time`), encoded in the result value type.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfLastOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesTsOfFirstOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
At each grid point, returns the timestamp of the earliest sample in the window as seconds, encoded in the result value type.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfFirstOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesTsOfMinOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
At each grid point, returns the timestamp (as seconds) of the sample whose value is the minimum over the window; ties are resolved by the aggregate implementation.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfMinOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesTsOfMaxOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
At each grid point, returns the timestamp (as seconds) of the sample whose value is the maximum over the window; ties are resolved by the aggregate implementation.
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesTsOfMaxOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ true, AggregateFunctionTimeseriesQuantileOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
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
        {createAggregateFunctionTimeseries</* is_rate_or_resets = */ false, /* is_predict = */ false, /* is_quantile = */ false, AggregateFunctionTimeseriesMadOverTimeTraits, AggregateFunctionTimeseriesOverTime>,
            makeOverTimeGridDocs(
                FunctionDocumentation::Description(R"(
Computes median absolute deviation over the window, analogous to [PromQL `mad_over_time`](https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time).
)"),
                FunctionDocumentation::Syntax(R"(
timeSeriesMadOverTimeToGrid(start_timestamp, end_timestamp, grid_step, window)(timestamp, value)
    )"),
                parameters_timeseries_over_time_grid_base,
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
