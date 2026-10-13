#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <span>
#include <vector>

#include <base/defines.h>
#include <base/types.h>
#include <Interpreters/AdaptiveAggregation/AdaptivePartitionLayout.h>

namespace DB
{

/// The count bins of the top-K pruning (see `AdaptiveTopKPruning`) sit on the hash bits 14..31, the bucket's and the
/// ten right below them, so every bucket owns 1024 consecutive bins and every partition or merge unit a run of them. A
/// bin then holds a few hundred rows of a hundred-million-row aggregation, which keeps the bounds of most bins below
/// the counts of a top 10 even when its groups have only a few hundred rows each.
constexpr size_t adaptive_count_bins_per_bucket = 1024;
constexpr size_t adaptive_count_bins = ADAPTIVE_AGGREGATION_NUM_BUCKETS * adaptive_count_bins_per_bucket;

inline size_t adaptiveCountBin(UInt64 hash)
{
    return (hash >> 14) & (adaptive_count_bins - 1);
}

/// A count bin's place among the bins of its bucket.
inline size_t ALWAYS_INLINE bucketCountBin(UInt64 hash)
{
    return adaptiveCountBin(hash) & (adaptive_count_bins_per_bucket - 1);
}

/// A producer's count bins of the top-K pruning (see `AdaptiveTopKPruning`), allocated by its first freeze, or by its
/// finish if it never froze, and kept through the freezes that follow a write of its table to disk. The counters are
/// narrow, to keep the bins of a producer in its cache, and saturate: a saturated bin bounds nothing, but it holds that
/// many rows of one producer, so it is among the heaviest anyway.
class AdaptiveTopKCountBins
{
public:
    bool isAllocated() const { return bins != nullptr; }

    /// Allocates the zeroed bins.
    void allocate() { bins = std::make_unique<UInt16[]>(adaptive_count_bins); }

    /// Adds the rows of a contribution to the bin of its group's hash.
    ALWAYS_INLINE void add(UInt64 hash, UInt64 contribution)
    {
        UInt16 & bin = bins[adaptiveCountBin(hash)];
        bin = static_cast<UInt16>(std::min<UInt64>(UInt64{bin} + contribution, std::numeric_limits<UInt16>::max()));
    }

private:
    friend class AdaptiveTopKPruning;

    std::unique_ptr<UInt16[]> bins;
};

/// The bin-bound top-K pruning of an aggregation that feeds `ORDER BY count() DESC LIMIT n`, or the same by `uniqExact`
/// or `uniqExactIf` (`Aggregator::Params::bucket_top_k`). Every producer counts the rows of its
/// staged records, and at its finish the counts of the groups of its own table, into its bins. Summed over the
/// producers, a bin bounds the count of every group in it from above: a row adds one to a group's count at most, and
/// merging adds the counts of the merged groups for `count`, or at most adds them for the distinct count of
/// `uniqExact`, as a union has no more elements than its parts together. The merge keeps the `limit` best exact counts
/// of the groups it has converted; once there are `limit` of them, a group whose bin is bounded below the smallest
/// one has `limit` groups ahead of it, so the merge skips the units, staged records and source cells of such bins
/// without draining them. The merge takes the buckets with the largest bounds first, so the threshold rises early.
class AdaptiveTopKPruning
{
public:
    /// A producer's finished bins and the largest bin of each bucket.
    struct ProducerBins
    {
        AdaptiveTopKCountBins counts;
        std::array<UInt16, ADAPTIVE_AGGREGATION_NUM_BUCKETS> bucket_maxima{};
    };

    /// The bounds of the bins of a bucket.
    using BucketBounds = std::array<UInt64, adaptive_count_bins_per_bucket>;

    explicit AdaptiveTopKPruning(size_t limit_) : limit(limit_) { }

    /// Takes a producer's bins at its finish and finds the largest bin of each bucket. Called before the producer takes
    /// the session's lock, so the scans of the finishing producers run in parallel.
    static ProducerBins finalizeProducerCounts(AdaptiveTopKCountBins && counts);

    /// Hands a producer's finished bins over, under the session's `producer_buffers_mutex`.
    void addProducerBins(ProducerBins && bins) { producer_bins.push_back(std::move(bins)); }

    /// Called after the finish barrier, before the merge starts. Checks that `expected_producers` handed their bins
    /// over, which the bounds rely on, and orders the buckets for the merge by their bounds. Returns false if some
    /// producer did not hand its bins over: the pruning cannot be used then.
    bool prepare(size_t expected_producers);

    /// The bucket that the merge claim `claim` takes: the buckets with the largest bounds come first.
    UInt32 bucketToMerge(UInt32 claim) const { return bucket_order[claim]; }

    /// The bounds of a bucket's bins: their sums over the producers, unbounded where a producer's counter saturated.
    BucketBounds boundsForBucket(size_t bucket) const;

    /// The smallest of the `limit` best exact counts converted so far, or zero while there are fewer.
    ALWAYS_INLINE UInt64 currentThreshold() const { return threshold.load(std::memory_order_relaxed); }

    /// Offers the counts of a converted unit's groups to the best counts, and publishes the smallest of them as the
    /// threshold once there are `limit`.
    void offerCounts(std::span<const UInt64> counts);

private:
    const size_t limit;

    std::vector<ProducerBins> producer_bins;
    std::array<UInt8, ADAPTIVE_AGGREGATION_NUM_BUCKETS> bucket_order{};

    /// The `limit` best exact counts converted so far, and the smallest of them once there are `limit`.
    std::mutex best_mutex;
    std::priority_queue<UInt64, std::vector<UInt64>, std::greater<>> best;
    std::atomic<UInt64> threshold{0};
};

}
