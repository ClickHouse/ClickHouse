#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include <base/defines.h>
#include <base/types.h>
#include <Interpreters/AdaptiveAggregation/AdaptivePartitionLayout.h>

namespace DB
{

struct AggregatedDataVariants;

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

/// A producer's count bins of the top-K pruning (see `AdaptiveTopKPruning`), allocated by its first freeze and kept
/// through the freezes that follow a write of its table to disk. The counters are narrow, to keep the bins of a producer
/// in its cache, and saturate: a saturated bin bounds nothing, but it holds that many rows of one producer, so it is
/// among the heaviest anyway.
class AdaptiveTopKCountBins
{
public:
    bool isAllocated() const { return bins != nullptr; }

    /// Allocates the zeroed bins.
    void allocate() { bins = std::make_unique<UInt16[]>(adaptive_count_bins); }

    /// The rows in the bins that hold fewer than `bound` rows each. A saturated bin holds an unknown number
    /// of rows, so it is never among them.
    UInt64 rowsInBinsBelow(UInt64 bound) const
    {
        UInt64 rows = 0;
        for (size_t bin = 0; bin < adaptive_count_bins; ++bin)
            if (bins[bin] < bound && bins[bin] != std::numeric_limits<UInt16>::max())
                rows += bins[bin];
        return rows;
    }

    /// Appends the row count of every bin that holds rows to `counts`: for a saturated bin the largest
    /// counter value, which is the least it can hold.
    void appendNonEmpty(std::vector<UInt64> & counts) const
    {
        for (size_t bin = 0; bin < adaptive_count_bins; ++bin)
            if (bins[bin])
                counts.push_back(bins[bin]);
    }

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
/// or `uniqExactIf` (`Aggregator::Params::bucket_top_k`). Every producer that froze counts the rows of its staged
/// records, and at its finish the counts of the groups of its own table, into its bins. A producer that never froze
/// staged nothing, so its table is its whole contribution: it hands the table over without bins, with the largest count
/// of each bucket where the pruning can run without a freeze, and the merge of each bucket adds the table's groups in
/// the bucket to the bucket's bounds. An aggregation in which no producer froze takes the adaptive merge for the pruning
/// too when the pruning pays for its buckets (see `Aggregator::mergePrunesByTopK`), and the merge reads the
/// tables through their cell indexes. Summed over the producers, a bin bounds the count of every group in it from
/// above: a row adds one to a group's count at most, and merging adds the counts of the merged groups for `count`, or at
/// most adds them for the distinct count of `uniqExact`, as a union has no more elements than its parts together. A
/// group whose bin is bounded below the merge's threshold, the worst of the `n` best exact counts converted so far (see
/// `AdaptiveTopKThreshold`), has `n` groups ahead of it, so the merge skips the units, staged records and source cells
/// of such bins without draining them. The merge takes the buckets with the largest bounds first, so the threshold
/// rises early.
class AdaptiveTopKPruning
{
public:
    /// A producer's finished bins and the largest bin of each bucket.
    struct ProducerBins
    {
        AdaptiveTopKCountBins counts;
        std::array<UInt16, ADAPTIVE_AGGREGATION_NUM_BUCKETS> max_bin_per_bucket{};
    };

    /// The bounds of the bins of a bucket.
    using BucketBounds = std::array<UInt64, adaptive_count_bins_per_bucket>;

    /// The bins of a bucket that can still hold a group of the top (see `selectUnit`).
    using AliveBins = std::array<bool, adaptive_count_bins_per_bucket>;

    /// What the merge does with a merge unit at the threshold of the moment (see `selectUnit`).
    enum class UnitSelection : UInt8
    {
        /// No bin of the unit can hold a group of the top: the merge skips the unit.
        Skip,
        /// Every bin of the unit can: the merge drains all of it.
        KeepAll,
        /// Some bins can: the merge drains only the records and source cells of the bins marked alive.
        Filter,
    };

    /// Takes a producer's bins at its finish and finds the largest bin of each bucket. Called before the producer takes
    /// the session's lock, so the scans of the finishing producers run in parallel.
    static ProducerBins finalizeProducerCounts(AdaptiveTopKCountBins && counts);

    /// Hands a producer's finished bins over, under the session's `producer_buffers_mutex`, and adds its
    /// largest bin of each bucket to the bucket's bound.
    void addProducerBins(ProducerBins && bins)
    {
        for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
            count_bound_per_bucket[bucket] += bins.max_bin_per_bucket[bucket];
        producer_bins.push_back(std::move(bins.counts));
    }

    /// Hands over the table of a producer that never froze, under the session's `producer_buffers_mutex`. The
    /// merge adds its groups to the bounds (see `addToBounds`). `max_count_per_bucket`, unless null, holds
    /// the largest count of a group of each bucket in the table, which the producer measures where the
    /// pruning can run without a freeze (see `Aggregator::adaptiveTopKPrunesWithoutFreeze`). A table without
    /// it takes no part in the order of the buckets.
    void addMergeCountedTable(
        const AggregatedDataVariants & table, const std::array<UInt64, ADAPTIVE_AGGREGATION_NUM_BUCKETS> * max_count_per_bucket)
    {
        merge_counted_tables.push_back(&table);
        if (max_count_per_bucket)
            for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
                count_bound_per_bucket[bucket] += (*max_count_per_bucket)[bucket];
    }

    /// Whether `table` is the table of a producer that never froze, whose groups the merge adds to the bounds.
    bool isMergeCounted(const AggregatedDataVariants & table) const
    {
        return std::ranges::find(merge_counted_tables, &table) != merge_counted_tables.end();
    }

    /// Whether all `producers` handed their bins or their tables over, which the bounds rely on: if some
    /// producer handed over neither, the pruning cannot be used. Called after the finish barrier, before the
    /// merge starts.
    bool countsEveryProducer(size_t producers) const { return producer_bins.size() + merge_counted_tables.size() == producers; }

    /// Orders the buckets for the merge by their bounds, `count_bound_per_bucket`. A table without the
    /// largest counts of its buckets is not in this order, which only lets the threshold rise later. Called
    /// once, after the finish barrier and before the merge starts.
    void orderBuckets();

    /// The bucket that the merge claim `claim` takes: the buckets with the largest bounds come first.
    UInt32 bucketToMerge(UInt32 claim) const { return bucket_order[claim]; }

    /// The bounds of a bucket's bins: their sums over the producers' bins, unbounded where a producer's counter
    /// saturated. The merge adds the groups of the tables it counts.
    BucketBounds boundsForBucket(size_t bucket) const;

    /// Adds the count of a group of `hash` to the bounds of its bucket, saturating at the unbounded value.
    static ALWAYS_INLINE void addToBounds(std::span<UInt64> bounds, UInt64 hash, UInt64 count)
    {
        UInt64 & bound = bounds[bucketCountBin(hash)];
        bound = count > std::numeric_limits<UInt64>::max() - bound ? std::numeric_limits<UInt64>::max() : bound + count;
    }

    /// Selects what the merge does with unit `unit` of the `units` that split the bucket, each a run of its partitions
    /// and so of its bins, at the merge's threshold of the moment, `threshold`: a bin can hold a group of the
    /// top while its bound in `bounds` reaches the threshold. Marks the unit's bins that can in `alive`,
    /// which the merge reads for `Filter`.
    UnitSelection selectUnit(const BucketBounds & bounds, size_t unit, size_t units, UInt64 threshold, AliveBins & alive) const;

private:
    /// The finished bins of the producers that froze, which the bounds of the bins sum (see
    /// `boundsForBucket`).
    std::vector<AdaptiveTopKCountBins> producer_bins;
    std::vector<const AggregatedDataVariants *> merge_counted_tables;
    /// The sum of every producer's largest bin in each bucket and of the bucket's largest count in every table
    /// that the merge counts and that brought it, which orders the buckets for the merge. It bounds the count
    /// of every group of the bucket when every such table brought its largest counts: a bucket's largest bin
    /// is at most the sum of the producers' largest bins in it, and a group of the bucket counts at most the
    /// sum of the largest counts of the bucket in those tables.
    std::array<UInt64, ADAPTIVE_AGGREGATION_NUM_BUCKETS> count_bound_per_bucket{};
    std::array<UInt8, ADAPTIVE_AGGREGATION_NUM_BUCKETS> bucket_order{};
};

}
