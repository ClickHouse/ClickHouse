#pragma once

#include <array>
#include <memory>
#include <mutex>
#include <variant>
#include <vector>

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HyperLogLogCounter.h>
#include <Common/PODArray.h>
#include <Common/PartitionedRecordBuffer.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregation.h>
#include <Interpreters/AdaptiveAggregation/AdaptivePartitionLayout.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveTopKPruning.h>
#include <Interpreters/AdaptiveAggregation/StagedRecords.h>
#include <Interpreters/TemporaryDataOnDisk.h>

namespace DB
{

/// Tuning of the adaptive aggregation.
/// The probe bypass decides after this many post-freeze rows per thread. It must be reachable
/// by every stream: at 64 threads a thread sees 1/64th of the input, so a filtered ~13M-row
/// aggregation still leaves ~200K rows per thread.
constexpr size_t adaptive_bypass_sample_rows = 65'536;
/// Probing the frozen table pays off only while at least one row in this many hits it.
constexpr size_t adaptive_bypass_hit_rate_inverse = 4;
/// Lookahead of the append-position prefetch into the producer's partitions (see `appendDelayedRecords`).
constexpr size_t adaptive_append_prefetch_distance = 8;
/// The staged records of this many consecutive buckets share allocation blocks, so their memory is released as the
/// merge tasks advance through the bucket order, while the grouping holds when the pruning reorders the buckets.
constexpr size_t adaptive_staging_block_buckets = 16;
/// Partition-count combining starts with a bounded trial of nonconsecutive misses in each block.
constexpr size_t adaptive_count_combining_sample_records = 1024;
/// Combining continues only if at least one lookup in this many reuses a missed key's count descriptor.
constexpr size_t adaptive_count_combining_min_hit_rate_inverse = 8;
/// Fixed-width arguments are gathered column by column into bounded batches of staged records, so
/// width and nullability are dispatched once per field while the destination rows remain in cache.
constexpr size_t adaptive_argument_staging_batch_rows = 1024;
/// Fixed lookahead of the drain's hash prefetch.
constexpr size_t adaptive_drain_prefetch_look_ahead = 16;
/// How much of the next range of records the drain requests ahead of its prefetch cursor: a page, the most a
/// hardware stream prefetcher covers without a fresh start.
constexpr size_t adaptive_drain_range_prefetch_bytes = 4096;
/// A merge unit's table is prefetched into only when it is larger than this: below it the table stays in the
/// cache through the drain, and a prefetch of a cache-resident slot is pure overhead.
constexpr size_t adaptive_drain_prefetch_min_table_bytes = 512 << 10;
/// A producer over the external-aggregation threshold spills its staged records only once it holds this much of
/// them, or a share of the threshold where that is less: a spill writes one block per partition and takes a lock per
/// bucket, so a producer that just spilled must not spill its few new records on every block that follows.
constexpr size_t adaptive_spill_min_bytes = 16 << 20;
/// The spill streams of a session, one per bucket, are open together, and each holds about three buffers of the
/// streams' buffer size: the compression input, the compressed block and the file's write buffer. Their buffer size
/// therefore follows the external-aggregation threshold, so that the streams together take at most an eighth of it,
/// but not below this floor, under which the writes and the compressed blocks get too small, and not above the buffer
/// size configured for temporary files.
constexpr size_t adaptive_spill_min_buffer_bytes = 4 << 10;
/// The merge takes a bucket's partitions in units of about this many records and source cells each, so a unit's
/// table, grown to hold them, stays in the cache while the unit is drained and converted; a bucket smaller than a
/// unit is merged as one, which spares a small bucket the fixed cost of every further unit.
constexpr size_t adaptive_merge_unit_records = 16'384;
/// Groups below this work estimate use the existing parallelism across merge buckets. Above the upper
/// bound, a group has enough work to amortize additional merge tasks even while other buckets are busy.
/// Between the bounds, the threshold follows the estimated work per worker for this query.
constexpr size_t adaptive_parallel_merge_min_work = 16'384;
constexpr size_t adaptive_parallel_merge_max_work = 1'000'000;
/// A frozen table whose aggregate states own heap memory is written to disk over the external-aggregation threshold
/// only once it has absorbed this many rows since it froze (see `Aggregator::adaptiveFrozenTableWorthSpilling`): the
/// write empties the table, which then learns its keys anew, so a table that was just written must not be written
/// again on every block while the query stays over the threshold.
constexpr size_t adaptive_frozen_spill_min_hits = 65'536;

/// The thaw guard: the table filled and froze, but the stream behind it keeps repeating the same missing
/// keys instead of bringing rare ones. Staged misses are supposed to be rare keys, each staged about once.
/// A key's first staged record is the price of storing it once, repaid by the merge working on deduplicated
/// keys; repeats can instead be absorbed by in-place updates. The verdict weighs the repeated records'
/// bytes against the state retained by an ordinary table. Cheap, fixed-size states use the calibrated
/// minimum cost below. Larger inline states raise that cost, and a supported model of state growth adds
/// the estimated distinct payload. Switching mid-stream must pay for retaining the records already staged,
/// so the thaw multiplies the whole state cost by four. For cheap states this gives a bound of 300 bytes.
///
/// Each thread decides on its own stream: its baseline table would absorb only the repeats it sees itself,
/// and a key that every thread stages once costs no more than the one cell per thread the baseline would
/// keep. The repeat factor is estimated from a sparse sample of distinct staged hashes and the exact number
/// of staged records. The repeats weigh only while the staged records are at least a share of the rows the
/// frozen table saw (`adaptive_thaw_staged_share_inverse`): a table that absorbs nearly every row in place
/// loses little to a sliver of repeated misses, and its thaw would give up the frozen table's merge.
///
/// The weighting separates the shapes by how much a repeat costs. With an accurate cardinality estimate,
/// a near-unique stream has repeat ~ 1 and wasted bytes ~ 0 regardless of the records' size. A stream of
/// narrow fixed-width records pays ~ 24 bytes per repeat (a numeric key, a small argument and bookkeeping),
/// so it crosses the bound only past repeat ~ 13, where the pathological mid-cardinality streams live.
/// Wide keys or wide arguments pay the whole payload per repeat, so ~ 100-byte records cross already
/// at repeat ~ 4.
/// For cheap states, the bound of 300 splits the measured shapes: every shape that wants the thaw wastes
/// at least ~ 440 bytes per key (a 90-byte string key at repeat ~ 3, a 90-byte string argument at repeat ~ 5,
/// high-repeat count streams land in the kilobytes), and every shape that wins when kept engaged wastes
/// at most ~ 275 (fixed-width arguments up to repeat ~ 12.5, count streams far below).
/// `adaptive_thaw_min_staged_records` is the evidence floor of a thread before its verdict may fire. It is
/// in records rather than bytes because the repeat estimate's confidence comes from sampled observations.
constexpr UInt64 adaptive_thaw_sample_mask = 0xFF;
constexpr size_t adaptive_thaw_min_staged_records = 65'536;
constexpr size_t adaptive_thaw_state_cost_multiplier = 4;
constexpr size_t adaptive_thaw_staged_share_inverse = 4;

/// The verdict of a run, which the hash-table statistics keep for the later runs of the query (see
/// `Aggregator::adaptiveStagingVerdict`), weighs the whole staged stream of a thread that is still frozen
/// at its finish using the staging-cost comparison of the thaw guard. A thaw switches a thread in the
/// middle of its stream: the records staged so far stay, and the table starts filling only then, so it
/// pays only for heavy repeats. A verdict decides the next runs from their start, which have nothing to
/// switch, so the stream needs to repeat only enough for the ordinary path to win. The verdict estimates
/// the ordinary table's per-key cost from its hash buffer and arena allocations at the freeze, including
/// capacity and allocation overhead. Its state cost has a floor of 75 bytes per key, which splits the
/// cheap-state shapes measured per thread: the numeric-key streams that lose to the ordinary path when
/// kept engaged waste ~ 84 bytes per key and more (a count, a sum or a key-only stream at repeat ~ 6-10
/// in a thread), and those that win waste at most ~ 34 (the same streams at repeat ~ 1.5-3). Narrow count
/// and key-only streams at repeat ~ 2.5-3 still lose up to ~ 15% below the bound: the ordinary path of an
/// aggregation without states wins at less waste than the bytes tell.
constexpr size_t adaptive_staging_min_state_bytes_per_key = 75;

struct AdaptiveAggregationSession
{
    AdaptiveAggregationSession(
        AdaptivePartitionLayout layout_,
        TemporaryDataOnDiskScopePtr spill_scope_,
        std::unique_ptr<AdaptiveTopKPruning> top_k_pruning_)
        : layout(layout_)
        , spill_scope(std::move(spill_scope_))
        , top_k_pruning(std::move(top_k_pruning_))
    {
    }

    /// Maps a merge claim to its bucket, ordered by pruning bounds when pruning is enabled.
    UInt32 bucketToMerge(UInt32 claim) const;

    /// Whether some producer froze, which makes the merge adaptive. Read after the finish barrier.
    bool engaged() const { return frozen_producers > 0; }

    /// Every producer uses the layout chosen for the pipeline's aggregation streams.
    const AdaptivePartitionLayout layout;

    /// The aggregation's temporary data scope with the buffer size of the spill streams (see
    /// `adaptive_spill_min_buffer_bytes`), when the aggregation may spill.
    const TemporaryDataOnDiskScopePtr spill_scope;

    /// Set when the aggregation feeds `ORDER BY count() DESC LIMIT n`, or the same by `uniqExact` or `uniqExactIf` (see
    /// `AdaptiveTopKPruning`). The producers hand their count bins over to it, and after the finish barrier
    /// `Aggregator::prepareAdaptiveTopKPruning` keeps it only for a merge that can prune.
    std::unique_ptr<AdaptiveTopKPruning> top_k_pruning;

    /// The producers' staged records, handed over by each producer when it finishes (see
    /// `Aggregator::finishAdaptiveProducer`). The merge tasks read them without the mutex: they are created after
    /// the finish barrier, which ordered every hand-over before them.
    std::mutex producer_buffers_mutex;
    std::vector<std::unique_ptr<PartitionedRecordBuffer>> producer_buffers;

    /// Published under the same mutex and read after the finish barrier. Retained states contribute their
    /// merge-work estimates, and each staged record contributes one unit per aggregate using this merge.
    /// The total and the producer count estimate each worker's share of the merge.
    size_t estimated_merge_work = 0;
    size_t finished_producers = 0;

    /// The producers whose tables froze at least once, and those of them whose staged streams were repeat-dominated:
    /// they thawed, or they finished frozen past the bound of the verdict. Published under the same mutex, they make
    /// the verdict of the run (see `Aggregator::adaptiveStagingVerdict`).
    size_t frozen_producers = 0;
    size_t repeat_dominated_producers = 0;

    /// The staged records the producers spilled over the external-aggregation threshold (see
    /// `Aggregator::spillAdaptivePartitions`): one raw stream per bucket, shared by the producers and created by the
    /// first spill into it. A stream is a sequence of partition blocks, {UInt32 partition within the bucket, UInt64
    /// records, UInt64 bytes, the records}; the merge task of the bucket reads it back after the finish barrier.
    struct SpilledBucket
    {
        std::mutex mutex;
        std::unique_ptr<TemporaryDataBuffer> stream;
    };
    std::array<SpilledBucket, ADAPTIVE_AGGREGATION_NUM_BUCKETS> spilled_buckets;
};

/// The working memory of one adaptive merge task, kept across the buckets it merges so their units do not allocate
/// it again: the places and the source places of a unit's merge, the record pointers and ranges of a partition, and
/// for a count-first unit the hashes of its best groups, the counts an ascending selection keeps, and the records of
/// its best groups.
struct AdaptiveMergeScratch
{
    PaddedPODArray<AggregateDataPtr> places;
    PaddedPODArray<AggregateDataPtr> source_places;
    /// The merges of `source_places` into `places` ordered by their destination, and one destination's states, for the
    /// pool-parallel merge of a group many producers hold (see `Aggregator::mergeAdaptiveSourceStates`).
    PaddedPODArray<UInt32> merge_order;
    AggregateDataPtrs merge_group;
    RowStorePointers records;
    AdaptiveRecordRanges ranges;
    std::vector<std::pair<UInt64, UInt64>> best_counts_and_hashes;
    PaddedPODArray<UInt64> best_hashes;
    AdaptiveRecordRanges best_records;
};

/// Per-transform context of the adaptive aggregation: the thread's lifecycle phase and its
/// phase-owned counters, its staged records, and the current block's misses (the arrays are cleared but keep their
/// capacity across blocks).
struct AdaptiveAggregationProducer
{
    explicit AdaptiveAggregationProducer(AdaptiveAggregationSessionPtr shared_) : session(std::move(shared_)) { }

    /// The thread starts learning: the local table inserts as usual while the freeze rule
    /// watches its growth (see `executeOnBlock`). A frozen table that was written to disk
    /// under memory pressure learns again from empty.
    struct LearningState
    {
    };

    /// The adaptive phase proper: the local table only updates the keys it already holds
    /// and misses are staged for the merge. Carries the post-freeze hit-rate
    /// sampling: when the frozen table turns out to hold almost none of the stream's keys
    /// (a uniform high-cardinality distribution), probing it is pure overhead on every row;
    /// after the sample window the kernel switches to staging ordinary blocks without the lookup.
    /// Constant-key blocks still look up their one key, so they can keep growing the retained states
    /// even after per-row probes are bypassed. All hits count toward the frozen-table spill guard
    /// (see `adaptive_frozen_spill_min_hits`).
    struct FrozenState
    {
        explicit FrozenState(size_t estimated_table_bytes_per_key_)
            : estimated_table_bytes_per_key(estimated_table_bytes_per_key_)
        {
        }

        /// Estimated hash-table buffer and arena bytes per key. A populated table supplies its measured
        /// allocations, including capacity and allocation overhead. An empty table uses the aggregate state
        /// size as a lower bound. This excludes staged records, other workers' memory and allocations owned
        /// directly by aggregate states, matching `AggregatedDataVariants::allocatedBytes`.
        size_t estimated_table_bytes_per_key;
        size_t absorbed_rows = 0;
        size_t sampled_rows = 0;
        bool bypass_local_probe = false;

        /// Records the rows represented by local lookups and returns whether per-row probing was just bypassed.
        /// Constant-key blocks retain a single lookup after the sample ends, so their hits continue to count
        /// toward state growth while the hit-rate sample stays closed.
        bool recordLocalProbeResults(size_t hits, size_t probe_rows)
        {
            absorbed_rows += hits;
            if (bypass_local_probe)
                return false;
            sampled_rows += probe_rows;
            if (sampled_rows >= adaptive_bypass_sample_rows
                && absorbed_rows * adaptive_bypass_hit_rate_inverse < sampled_rows)
            {
                bypass_local_probe = true;
                return true;
            }
            return false;
        }

        /// The thaw evidence of this thread (see `Aggregator::adaptiveStagingRepeats`): the rows the frozen table saw,
        /// the records it staged and their estimated footprint (key bytes, variable-width argument bytes and the
        /// per-record bookkeeping), and a sparse sample of distinct staged hashes. Scaling the sample estimates
        /// the number of keys, which the known record count turns into a repetition estimate.
        size_t rows = 0;
        size_t staged_records = 0;
        size_t staged_bytes = 0;
        HashSet<UInt64> distinct_sampled_hashes;

        size_t getEstimatedStagedKeyCount() const
        {
            return distinct_sampled_hashes.size() * (adaptive_thaw_sample_mask + 1);
        }

        size_t getEstimatedDistinctInputCount() const
        {
            return distinct_sampled_inputs ? distinct_sampled_inputs->size() * (adaptive_thaw_sample_mask + 1) : 0;
        }

        /// Adds the hash of a sampled group key together with its aggregate argument tuple.
        void addSampledInputHash(UInt64 hash)
        {
            if (!distinct_sampled_inputs)
                distinct_sampled_inputs = std::make_unique<HyperLogLogCounter<12, UInt64, TrivialHash, UInt64>>();
            distinct_sampled_inputs->insert(hash);
        }

    private:
        /// Distinct argument tuples of the sampled keys estimate the payload a growing set state would retain.
        /// The sketch stays bounded even when a sampled key has arbitrarily many distinct arguments.
        std::unique_ptr<HyperLogLogCounter<12, UInt64, TrivialHash, UInt64>> distinct_sampled_inputs;
    };

    /// Terminal: the thread aggregates exactly as with the feature off. It stands down at its
    /// thaw, once its own staged stream proves to repeat its misses (see
    /// `Aggregator::adaptiveStagingRepeats`).
    struct BaselineState
    {
    };

    using Phase = std::variant<LearningState, FrozenState, BaselineState>;
    Phase phase = LearningState{};

    bool isLearning() const { return std::holds_alternative<LearningState>(phase); }
    bool isFrozen() const { return std::holds_alternative<FrozenState>(phase); }
    bool isBaseline() const { return std::holds_alternative<BaselineState>(phase); }

    void freeze(size_t estimated_table_bytes_per_key)
    {
        phase.emplace<FrozenState>(estimated_table_bytes_per_key);
    }
    void learnAgain() { phase = LearningState{}; }
    void standDown() { phase = BaselineState{}; }

    AdaptiveAggregationSessionPtr session;

    /// The records this producer staged, created by its freeze; a producer that stands down keeps them for the
    /// merge. Handed over to the session when the producer finishes.
    std::unique_ptr<PartitionedRecordBuffer> partitions;
    /// The producer's staged records, including those already written to disk, for the merge-work estimate.
    size_t total_staged_records = 0;

    /// The current block's misses, one entry per delayed record, in staging order: the source row, the routing hash,
    /// the run length of a count record, and the key, as its size for a string-like key or its value for a
    /// fixed-width one (`miss_keys`, the raw bytes of one key per record).
    PaddedPODArray<UInt32> miss_source_rows;
    PaddedPODArray<UInt64> miss_hashes;
    PaddedPODArray<UInt64> miss_key_sizes;
    PaddedPODArray<char> miss_keys;
    PaddedPODArray<UInt32> miss_multiplicities;

    /// The producer's count bins when the aggregation prunes by the top-K (see `AdaptiveTopKPruning`), created by its
    /// freeze, or by its finish if it never froze, and handed over at the finish.
    std::unique_ptr<UInt16[]> count_bins;
};

}
