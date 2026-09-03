/// Measures the two constants a non-invertible `timeSeries*ToGrid` function uses to choose between recomputing
/// each window and the sliding two-stack queue:
///   - `AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS`: the crossover - the smallest number of populated buckets in a
///     window at which two-stacks beats recompute. `createAggregator` compares the AVERAGE populated buckets per
///     window (`buckets_per_window * density`) against it, so it is the right value for roughly uniform data.
///   - `BPW_TO_FORCE_TWO_STACKS`: a cap on `buckets_per_window` above which two-stacks is used regardless of
///     average density. The average can hide a locally dense window (density is not uniform), and a fully dense
///     window is recompute's worst case; the cap is where that worst case is 2x slower than two-stacks, bounding
///     the damage a dense cluster can do.
///
/// Run:   `clickhouse-examples timeseries_to_grid_two_stack_vs_recompute`

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesLinearRegression.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>

#include <Common/Stopwatch.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <Examples/clickhouse_examples.h>

#include <fmt/format.h>

#include <algorithm>
#include <limits>
#include <vector>

namespace
{

using namespace DB;

/// Independent series measured in sequence. Only affects timing stability and adds a little data variety.
constexpr size_t NUM_SERIES = 32;

/// Populated buckets per series; with one per grid point the index range is also this.
constexpr size_t BASE_GRID = 8000;
constexpr Int64 STEP = 10;

/// Number of iterations to find minimum time.
constexpr int REPEATS = 3;

/// buckets_per_window values for the sweep, with finer resolution where the crossover and 2x cap fall.
constexpr size_t WINDOWS[]
    = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 28, 32, 36, 40};

/// Every non-invertible sliding aggregator stores its samples in this bucket type (one sample per step here).
using Bucket = AggregateFunctionTimeseriesSamples<UInt32, Float64>;
using Buckets = UnorderedMapWithMemoryTracking<size_t, Bucket>;
using Dataset = std::vector<Buckets>;  /// one Buckets map per series; STYLE_CHECK_ALLOW_STD_CONTAINERS

/// Creates NUM_SERIES series, each with BASE_GRID single-sample buckets at indices 0..BASE_GRID-1 (dense).
Dataset buildDataset()
{
    Dataset dataset(NUM_SERIES);
    for (size_t series = 0; series < NUM_SERIES; ++series)
    {
        Buckets & buckets = dataset[series];
        buckets.reserve(BASE_GRID);
        for (size_t k = 0; k < BASE_GRID; ++k)
        {
            Bucket bucket;
            bucket.add(static_cast<UInt32>(static_cast<Int64>(k) * STEP), static_cast<Float64>((k * 7 + series * 3) % 101));
            buckets.emplace(k, std::move(bucket));
        }
    }
    return dataset;
}

/// Returns the best (minimum over REPEATS) ns per grid point for sliding a window of `buckets_per_window` buckets.
/// `stack_size` controls whether it's two-stack (stack_size > 0) or recompute algorithm (stack_size == 0).
template <typename MakeAggregator>
Float64 measureNanoseconds(size_t buckets_per_window, size_t stack_size, const Dataset & dataset,
    const MakeAggregator & make_aggregator, Float64 & checksum)
{
    Float64 best = std::numeric_limits<Float64>::infinity();
    for (int r = 0; r < REPEATS; ++r)
    {
        Stopwatch stopwatch;
        for (const auto & buckets : dataset)
        {
            auto aggregator = make_aggregator(stack_size);
            for (size_t i = 0; i < BASE_GRID; ++i)
            {
                const auto it = buckets.find(i);
                if (it != buckets.end())
                    aggregator.add(it->second, static_cast<UInt32>(static_cast<Int64>(i) * STEP));
                if (i >= buckets_per_window)
                    aggregator.removeBefore(static_cast<UInt32>((static_cast<Int64>(i) - static_cast<Int64>(buckets_per_window)) * STEP));
                const auto result = aggregator.getResult(static_cast<UInt32>(static_cast<Int64>(i) * STEP));
                if (result)
                    checksum += static_cast<Float64>(*result);     /// read the result so the work cannot be optimised away
            }
        }
        const Float64 total_points = static_cast<Float64>(BASE_GRID) * static_cast<Float64>(dataset.size());
        best = std::min(best, static_cast<Float64>(stopwatch.elapsedNanoseconds()) / total_points);
    }
    return best;
}

/// Finds AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS and BPW_TO_FORCE_TWO_STACKS
/// for a specified aggregator.
template <typename MakeAggregator>
void runFunction(const char * name, const Dataset & dataset, const MakeAggregator & make_aggregator, Float64 & checksum)
{
    fmt::println("\n{}: two-stacks vs recompute, ns per grid point ({} series x {} buckets).\n",
        name, NUM_SERIES, BASE_GRID);
    fmt::println("{:>18}  {:>14}  {:>14}  {:>30}  {:>10}",
        "buckets_per_window", "recompute(ns)", "two_stacks(ns)", "ratio (recompute / two_stacks)", "winner");

    size_t two_stacks_faster_bpw = 0;    /// crossover: two-stacks first wins
    size_t two_stacks_2x_faster_bpw = 0;        /// recompute first >= MAX_SLOWDOWN_TIMES x slower
    for (size_t window : WINDOWS)
    {
        const Float64 recompute_ns = measureNanoseconds(window, /* stack_size */ 0, dataset, make_aggregator, checksum);
        const Float64 two_stacks_ns = measureNanoseconds(window, /* stack_size */ window, dataset, make_aggregator, checksum);
        const Float64 ratio = recompute_ns / two_stacks_ns;
        if (two_stacks_faster_bpw == 0 && ratio > 1.0)
            two_stacks_faster_bpw = window;
        if (two_stacks_2x_faster_bpw == 0 && ratio > 2.0)
            two_stacks_2x_faster_bpw = window;
        fmt::println("{:>18}  {:>14.2f}  {:>14.2f}  {:>30.2f}  {:>10}", window, recompute_ns, two_stacks_ns, ratio,
            ratio > 1.0 ? "two-stacks" : "recompute");
    }
    fmt::println("");
    fmt::println("AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS (two-stacks first wins)   = {}", two_stacks_faster_bpw);
    fmt::println("BPW_TO_FORCE_TWO_STACKS (two-stacks > 2x faster)                 = {}", two_stacks_2x_faster_bpw);
}

}

int mainEntryExampleTimeSeriesToGridTwoStackVsRecompute(int, char **)
{
    const Dataset dataset = buildDataset();
    Float64 checksum = 0;

    /// Linear regression (`timeSeriesDerivToGrid` / `timeSeriesPredictLinearToGrid` share the same `Summary`, so
    /// one measurement covers both).
    using LinearRegressionTraits = AggregateFunctionTimeseriesLinearRegressionTraits<UInt32, /* IntervalType */ Int32, /* ValueType */ Float64, /* is_predict */ false>;
    runFunction("timeSeriesDerivToGrid", dataset,
        [](size_t stack_size) { return LinearRegressionTraits::Aggregator{stack_size, /* base */ UInt32(0), /* predict_offset */ Float64(0)}; },
        checksum);

    /// Add other non-invertible functions here.

    /// Use the checksum so the measured work cannot be optimised away.
    if (checksum < 0)
        fmt::println("checksum {}", checksum);

    return 0;
}
