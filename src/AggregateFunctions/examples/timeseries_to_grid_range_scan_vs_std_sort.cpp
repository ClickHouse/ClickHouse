/// Measures `Base::BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN`: how a `timeSeries*ToGrid` finalize should visit its buckets in index
/// order. The buckets live in an `UnorderedMapWithMemoryTracking<index, Bucket>` keyed by an index in
/// `[0, bucket_count)`; finalize must process them in ascending index order. Two strategies, swept across
/// densities:
///   - range scan: walk index 0..bucket_count and `find` each (the "dense" path); O(bucket_count).
///   - collect + std::sort: gather the populated `(index, Bucket*)` and comparison-sort (the "sparse" path); O(n log n).
///
/// Each does the SAME per-bucket work (reads the bucket's sample), so the difference is the ordering cost
/// (reported as ns per populated bucket). The range scan wins when buckets are dense and degrades as they get
/// sparser; the smallest fill density `populated / bucket_count` at which it still beats `std::sort` is
/// `BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN` - i.e. use the range scan iff `populated / bucket_count >= factor`.
///
/// (An earlier version also measured ClickHouse's `radixSort`; `std::sort` beat it for this n and 16-byte
/// pointer-payload element, so radix was dropped.)
///
/// Driven over a production-shaped dataset (many series, buckets in node-based maps, working set > last-level
/// cache) so the cache behaviour matches the real query.
///
/// Build: it is part of the `clickhouse-examples` multi-call binary.
/// Run:   `clickhouse-examples timeseries_to_grid_range_scan_vs_std_sort`

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>

#include <Common/Stopwatch.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Examples/clickhouse_examples.h>

#include <fmt/format.h>
#include <pcg_random.hpp>

#include <algorithm>
#include <cmath>
#include <vector>

namespace
{

using namespace DB;

/// Populated buckets per series, kept constant across densities so the working set stays the same; the index
/// range is this divided by the density.
constexpr size_t BASE_GRID = 8000;
/// Independent series processed in sequence; sized so the whole dataset is well over the last-level cache.
constexpr size_t NUM_SERIES = 64;
constexpr Int64 STEP = 10;
constexpr int REPEATS = 5;  /// timing iterations per measurement; the minimum over them is used

using Bucket = AggregateFunctionTimeseriesSamples<UInt32, Float64>;
using Buckets = UnorderedMapWithMemoryTracking<size_t, Bucket>;
using Dataset = std::vector<Buckets>;  /// one Buckets map per series; STYLE_CHECK_ALLOW_STD_CONTAINERS

/// One populated bucket and its index, for the collect-and-sort strategy. Sorted by `index`.
struct IndexedBucket
{
    UInt32 index;
    const Bucket * bucket;
};

/// The common per-bucket work: read the bucket's sample(s), as the real `addBucket` does.
Float64 processBucket(const Bucket & bucket)
{
    Float64 sum = 0;
    bucket.forEachSample([&sum](UInt32, Float64 value) { sum += value; });
    return sum;
}

/// The slot range the scan walks for a given fill density: `round(BASE_GRID / density)`, so `populated /
/// bucket_count` (with populated = BASE_GRID) is ~= density.
size_t bucketCountForDensity(Float64 density)
{
    return static_cast<size_t>(std::llround(static_cast<Float64>(BASE_GRID) / density));
}

/// Places the BASE_GRID populated buckets at random distinct positions in `[0, bucket_count)`. A random
/// (irregular) layout matches real data - bucket indices come from sample timestamps, not a regular lattice -
/// and avoids the introsort cost artifact a regular layout triggers at some densities.
Dataset buildDataset(Float64 density)
{
    const size_t bucket_count = bucketCountForDensity(density);
    Dataset dataset(NUM_SERIES);
    for (size_t series = 0; series < NUM_SERIES; ++series)
    {
        pcg64_fast rng(series + 1);     /// deterministic per series
        Buckets & buckets = dataset[series];
        buckets.reserve(BASE_GRID);
        /// Floyd's algorithm: BASE_GRID distinct indices drawn uniformly from [0, bucket_count), in O(BASE_GRID).
        for (size_t i = bucket_count - BASE_GRID; i < bucket_count; ++i)
        {
            const size_t candidate = static_cast<size_t>(rng() % (i + 1));
            const size_t k = buckets.contains(candidate) ? i : candidate;
            Bucket bucket;
            bucket.add(static_cast<UInt32>(static_cast<Int64>(k) * STEP), static_cast<Float64>((k * 7 + series * 3) % 101));
            buckets.emplace(k, std::move(bucket));
        }
    }
    return dataset;
}

enum class Strategy { RangeScan, StdSort };

/// Best (minimum over REPEATS) ns per populated bucket for one strategy over the whole dataset.
Float64 measureNanoseconds(Strategy strategy, size_t bucket_count, const Dataset & dataset, Float64 & checksum)
{
    Float64 best = 1e30;
    for (int r = 0; r < REPEATS; ++r)
    {
        Stopwatch stopwatch;
        for (const auto & buckets : dataset)
        {
            if (strategy == Strategy::RangeScan)
            {
                for (size_t i = 0; i < bucket_count; ++i)
                {
                    const auto it = buckets.find(i);
                    if (it != buckets.end())
                        checksum += processBucket(it->second);
                }
            }
            else
            {
                /// Collect the populated buckets (a fresh vector per finalize, as production does), sort, process.
                VectorWithMemoryTracking<IndexedBucket> ordered;
                ordered.reserve(buckets.size());
                for (const auto & [index, bucket] : buckets)
                    ordered.push_back({static_cast<UInt32>(index), &bucket});
                std::sort(ordered.begin(), ordered.end(),
                    [](const IndexedBucket & lhs, const IndexedBucket & rhs) { return lhs.index < rhs.index; });
                for (const auto & indexed_bucket : ordered)
                    checksum += processBucket(*indexed_bucket.bucket);
            }
        }
        const Float64 populated = static_cast<Float64>(BASE_GRID) * static_cast<Float64>(NUM_SERIES);
        best = std::min(best, static_cast<Float64>(stopwatch.elapsedNanoseconds()) / populated);
    }
    return best;
}

}

int mainEntryExampleTimeSeriesToGridRangeScanVsStdSort(int, char **)
{
    fmt::println("Bucket iteration in index order, ns per populated bucket ({} series x {} populated buckets, > LLC).",
        NUM_SERIES, BASE_GRID);
    fmt::println("populated buckets is constant; bucket_count is the slot range the scan walks. Dense (range scan)");
    fmt::println("is chosen iff populated / bucket_count >= BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN.\n");
    fmt::println("{:>22}  {:>12}  {:>14}  {:>14}  {:>30}  {:>10}",
        "populated/bucket_count", "bucket_count", "range_scan(ns)", "std::sort(ns)", "ratio (range_scan / std::sort)", "winner");

    Float64 checksum = 0;
    Float64 dense_factor = 0;    /// smallest populated/bucket_count at which the range scan still wins (the crossover)
    bool range_still_winning = true;
    for (Float64 density : {1.0, 0.9, 0.8, 0.7, 0.6, 0.55, 0.5, 0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.15, 0.1})
    {
        const Dataset dataset = buildDataset(density);
        const size_t bucket_count = bucketCountForDensity(density);
        const Float64 range_scan_ns = measureNanoseconds(Strategy::RangeScan, bucket_count, dataset, checksum);
        const Float64 std_sort_ns = measureNanoseconds(Strategy::StdSort, bucket_count, dataset, checksum);
        const Float64 ratio = range_scan_ns / std_sort_ns;
        const bool range_wins = range_scan_ns < std_sort_ns;
        if (range_still_winning && range_wins)
            dense_factor = density;
        else
            range_still_winning = false;
        const Float64 actual_density = static_cast<Float64>(BASE_GRID) / static_cast<Float64>(bucket_count);
        fmt::println("{:>22.3f}  {:>12}  {:>14.2f}  {:>14.2f}  {:>30.2f}  {:>10}",
            actual_density, bucket_count, range_scan_ns, std_sort_ns, ratio, range_wins ? "range_scan" : "std::sort");
    }

    fmt::println("");
    fmt::println("BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN = {:.2f}", dense_factor);

    /// Use the checksum so the measured work cannot be optimised away.
    if (checksum < 0)
        fmt::println("checksum {}", checksum);

    return 0;
}
