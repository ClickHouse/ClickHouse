#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

#include <base/defines.h>
#include <base/types.h>
#include <Interpreters/AdaptiveAggregation/AdaptivePartitionLayout.h>

namespace DB
{

class IColumn;

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

/// The bin-bound top-K pruning of an aggregation that feeds `ORDER BY count() DESC LIMIT n`, or the same by `uniqExact`
/// or `uniqExactIf` (`Aggregator::Params::bucket_top_k`). Every producer counts the rows of its
/// staged records, and at its finish the counts of the groups of its own table, into its bins. Summed over the
/// producers, a bin bounds the count of every group in it from above: a row adds one to a group's count at most, and
/// merging adds the counts of the merged groups for `count`, or at most adds them for the distinct count of
/// `uniqExact`, as a union has no more elements than its parts together. The merge keeps the `limit` best exact counts
/// of the groups it has converted; once there are `limit` of them, a group whose bin is bounded below the smallest
/// one has `limit` groups ahead of it, so the merge skips the units, staged records and source cells of such bins
/// without draining them. The merge takes the buckets with the largest bounds first, so the threshold rises early.
struct AdaptiveTopKPruning
{
    explicit AdaptiveTopKPruning(size_t limit_) : limit(limit_) { }

    const size_t limit;

    /// A producer's bins and the largest bin of each bucket. The counters are narrow, to keep the bins of a producer in
    /// its cache, and saturate: a saturated bin bounds nothing, but it holds that many rows of one producer, so it is
    /// among the heaviest anyway.
    struct ProducerBins
    {
        std::unique_ptr<UInt16[]> bins;
        std::array<UInt16, ADAPTIVE_AGGREGATION_NUM_BUCKETS> bucket_maxima{};
    };

    /// Handed over by every producer at its finish, under the session's `producer_buffers_mutex`.
    std::vector<ProducerBins> producer_bins;

    /// The order the merge tasks claim the buckets in, set before the merge starts (see
    /// `Aggregator::prepareAdaptiveTopKPruning`).
    std::array<UInt8, ADAPTIVE_AGGREGATION_NUM_BUCKETS> bucket_order{};

    /// The `limit` best exact counts converted so far, and the smallest of them once there are `limit`.
    std::mutex best_mutex;
    std::priority_queue<UInt64, std::vector<UInt64>, std::greater<>> best;
    std::atomic<UInt64> threshold{0};
};

/// Adds rows to a count bin of the top-K pruning. The counter saturates, and a saturated bin bounds nothing.
inline void ALWAYS_INLINE addToCountBin(UInt16 & bin, UInt64 rows)
{
    bin = static_cast<UInt16>(std::min<UInt64>(UInt64{bin} + rows, std::numeric_limits<UInt16>::max()));
}

/// A count bin's place among the bins of its bucket.
inline size_t ALWAYS_INLINE bucketCountBin(UInt64 hash)
{
    return adaptiveCountBin(hash) & (adaptive_count_bins_per_bucket - 1);
}

/// The bounds of a bucket's bins: their sums over the producers, unbounded where a producer's counter saturated.
std::array<UInt64, adaptive_count_bins_per_bucket> sumBucketCountBins(const AdaptiveTopKPruning & pruning, size_t bucket);

/// Offers the counts of a converted unit's groups to the pruning's best counts, and publishes the smallest of them
/// as the threshold once there are `limit`.
void offerTopKCounts(AdaptiveTopKPruning & pruning, const IColumn & column);

}
