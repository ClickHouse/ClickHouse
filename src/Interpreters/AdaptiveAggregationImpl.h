#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <variant>
#include <vector>

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/HashSet.h>
#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/AdaptiveAggregation.h>
#include <Interpreters/AdaptivePartitions.h>
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
/// Lookaheads of the two-stage prefetch of the appends into the producer's partitions (see `appendDelayedRecords`).
constexpr size_t adaptive_append_cursor_prefetch_distance = 16;
constexpr size_t adaptive_append_prefetch_distance = 8;
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
/// The count bins of the top-K pruning (see `AdaptiveTopKPruning`) sit on the hash bits 14..31, the bucket's and the
/// ten right below them, so every bucket owns 1024 consecutive bins and every partition or merge unit a run of them. A
/// bin then holds a few hundred rows of a hundred-million-row aggregation, which keeps the bounds of most bins below
/// the counts of a top 10 even when its groups have only a few hundred rows each.
constexpr size_t adaptive_count_bins_per_bucket = 1024;
constexpr size_t adaptive_count_bins = ADAPTIVE_AGGREGATION_NUM_BUCKETS * adaptive_count_bins_per_bucket;
/// A thread gives up on freezing once it has consumed this many times the freeze threshold
/// in rows while holding fewer keys than the threshold. High-cardinality streams freeze
/// within a couple of blocks and skewed streams at roughly threshold / (1 - hot share) rows,
/// so only repeat-dominated tables (average multiplicity above the multiple) ever give up.
/// The value balances two bounds: it caps the tolerated hot share at 1 - 1/multiple (a 90%
/// hot key still freezes with a wide margin), and it must fire within the rows one thread
/// sees on a medium table at a wide fan-out (a 64-thread scan of 50M rows gives each thread
/// less than a million rows).
constexpr size_t adaptive_freeze_give_up_row_multiple = 16;

/// The thaw guard, for the failure the give-up cannot see: the table filled and froze, but
/// the stream behind it keeps repeating the same missing keys instead of bringing rare ones.
/// Staged misses are supposed to be rare keys, each staged about once. A key's first staged
/// record is the price of storing it once, repaid by the merge working on deduplicated keys;
/// every repeat is bytes the baseline would have absorbed as a cheap in-place update. The
/// verdict therefore weighs the repeats by the records' bytes: the stream thaws once the
/// wasted staged bytes per distinct key, (repeat factor - 1) * bytes per record, exceed the
/// bound. The repeat factor is estimated over a shared sparse sample of staged hashes.
/// Repeats of a key collapse onto one sample entry across all threads, so the estimate does
/// not depend on how a key's occurrences spread over the threads. The weighting separates the shapes by how
/// much a repeat costs. A near-unique stream has repeat ~ 1, so its wasted bytes are ~ 0 and
/// it can never fire, no matter how heavy its records are. A stream of narrow fixed-width
/// records pays ~ 24 bytes per repeat (a numeric key plus the bookkeeping), so it crosses the
/// bound only past repeat ~ 13, where the pathological mid-cardinality streams live. A stream
/// of wide keys or wide string arguments pays the whole record per repeat, so ~ 100-byte
/// records cross already at repeat ~ 4. The bound of 300 splits the measured shapes: every
/// shape that wants the thaw wastes at least ~ 440 bytes per key (a 90-byte string key at
/// repeat ~ 3, a 90-byte string argument at repeat ~ 5, high-repeat count streams land in the
/// kilobytes), and every shape that wins when kept engaged wastes at most ~ 275 (fixed-width
/// arguments up to repeat ~ 12.5, count streams far below). `adaptive_thaw_min_staged_records`
/// is the evidence floor before the verdict may fire. It is in records rather than bytes
/// because the repeat estimate's confidence comes from the number of sampled observations.
constexpr UInt64 adaptive_thaw_sample_mask = 0xFF;
constexpr size_t adaptive_thaw_min_staged_records = 524'288;
constexpr size_t adaptive_thaw_wasted_bytes_per_key = 300;

/// The record layout of the aggregate arguments that general payloads stage, fixed for the query by the header.
/// The fixed-size arguments come first, at fixed offsets and in the form `RowDataStore` uses (a Nullable field is a
/// null byte followed by the value), so the drain rebuilds each of them with one `IColumn::fillFromRowStorePtrs`
/// over the records; the variable-size arguments follow the key, serialized one after another in this order. A
/// column read by several aggregates is staged once.
struct AdaptiveArgumentLayout
{
    struct FixedField
    {
        size_t position;
        DataTypePtr type;
        size_t offset;
        size_t size;
    };

    struct VariableField
    {
        size_t position;
        DataTypePtr type;
    };

    std::vector<FixedField> fixed_fields;
    std::vector<VariableField> variable_fields;
    size_t fixed_bytes = 0;
    /// One past the highest argument position, the size of the column vectors the positions index.
    size_t num_positions = 0;
};

inline size_t adaptiveCountBin(UInt64 hash)
{
    return (hash >> 14) & (adaptive_count_bins - 1);
}

/// The bin-bound top-K pruning of an aggregation that feeds `ORDER BY count() DESC LIMIT n`
/// (`Aggregator::Params::bucket_top_k`). Every producer counts the rows of its staged records, and at its finish the
/// rows of its own table, into its bins. Summed over the producers, a bin bounds the count of every group in it from
/// above: the rows of all its groups are in it. The merge keeps the `limit` best exact counts of the groups it has
/// converted; once there are `limit` of them, a group whose bin is bounded below the smallest one has `limit` groups
/// ahead of it, so the merge skips the units, staged records and source cells of such bins without draining them.
/// The merge takes the buckets with the largest bounds first, so the threshold rises early.
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
        std::array<UInt16, ADAPTIVE_AGGREGATION_NUM_BUCKETS> bucket_maxima;
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

struct AdaptiveAggregationSession
{
    std::once_flag init_flag;
    std::atomic<bool> initialized{false};

    /// The partitioning of every producer's staged records, fixed by the first freeze.
    AdaptivePartitionLayout layout;

    /// The producers' staged records, handed over by each producer when it finishes (see
    /// `Aggregator::finishAdaptiveProducer`). The merge tasks read them without the mutex: they are created after
    /// the finish barrier, which ordered every hand-over before them.
    std::mutex producer_buffers_mutex;
    std::vector<AdaptivePartitionBuffersPtr> producer_buffers;

    /// The aggregation's temporary data scope with the buffer size of the spill streams (see
    /// `adaptive_spill_min_buffer_bytes`), set by the first freeze when the aggregation may spill.
    TemporaryDataOnDiskScopePtr spill_scope;

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

    /// The thaw sampler (see the tuning constants above). The producers fold a sparse sample of
    /// their staged record hashes in here; repeats of a key collapse onto one entry across all
    /// threads, so sampled records per distinct sampled hash estimates the repeat factor of the
    /// staged stream as a whole, independently of how a key's occurrences spread over the threads.
    std::mutex thaw_sample_mutex;
    HashSet<UInt64> distinct_sampled_hashes;
    size_t thaw_sampled_records = 0;
    size_t staged_records = 0;
    /// The staged records' estimated footprint: key bytes, variable-width argument bytes and the per-record
    /// bookkeeping. It measures the same records as `staged_records` and the sample.
    size_t staged_bytes = 0;
    /// Set once the staged stream proves repeat-dominated; every thread then thaws its local
    /// table at the next block and returns to the baseline path for good.
    std::atomic<bool> thaw_all{false};

    /// Set by the first freeze when the aggregation feeds `ORDER BY count() DESC LIMIT n` (see
    /// `AdaptiveTopKPruning`).
    std::unique_ptr<AdaptiveTopKPruning> top_k_pruning;
};

using AdaptiveAggregationSessionPtr = std::shared_ptr<AdaptiveAggregationSession>;

/// The working memory of one adaptive merge task, kept across the buckets it merges so their units do not allocate
/// it again: the places and the source places of a unit's merge, and the record pointers and ranges of a partition.
struct AdaptiveMergeScratch
{
    PaddedPODArray<AggregateDataPtr> places;
    PaddedPODArray<AggregateDataPtr> source_places;
    RowStorePointers records;
    AdaptiveRecordRanges ranges;
};

/// Per-transform context of the adaptive aggregation: the thread's lifecycle phase and its
/// phase-owned counters, its staged records, and the current block's misses (the arrays are cleared but keep their
/// capacity across blocks).
struct AdaptiveAggregationProducer
{
    explicit AdaptiveAggregationProducer(AdaptiveAggregationSessionPtr shared_) : session(std::move(shared_)) { }

    /// The thread starts learning: the local table inserts as usual while the freeze rule
    /// watches its growth. Rows consumed here feed the give-up rule (see `executeOnBlock`).
    struct LearningState
    {
        size_t rows_seen = 0;
    };

    /// The adaptive phase proper: the local table only updates the keys it already holds
    /// and misses are staged for the merge. Carries the post-freeze hit-rate
    /// sampling: when the frozen table turns out to hold almost none of the stream's keys
    /// (a uniform high-cardinality distribution), probing it is pure overhead on every row;
    /// after the sample window the kernel switches to staging every row without the lookup.
    struct FrozenState
    {
        size_t sampled_rows = 0;
        size_t sampled_hits = 0;
        bool bypass_local_probe = false;
    };

    /// Terminal: the thread aggregates exactly as with the feature off, keeping only the
    /// reason it stood down.
    struct BaselineState
    {
        enum class Reason
        {
            /// The give-up rule: the table stayed far below the freeze threshold across
            /// many times that many rows, so the stream is repeat-dominated locally.
            TooFewDistinctKeys,
            /// The global thaw: the session-wide staged-key sample proved the whole stream
            /// repeat-dominated (see `appendDelayedRecords`).
            RepeatedStagedKeys,
        };
        Reason reason;
    };

    using Phase = std::variant<LearningState, FrozenState, BaselineState>;
    Phase phase = LearningState{};

    bool isLearning() const { return std::holds_alternative<LearningState>(phase); }
    bool isFrozen() const { return std::holds_alternative<FrozenState>(phase); }
    bool isBaseline() const { return std::holds_alternative<BaselineState>(phase); }

    void freeze() { phase = FrozenState{}; }
    void standDown(BaselineState::Reason reason) { phase = BaselineState{.reason = reason}; }

    AdaptiveAggregationSessionPtr session;

    /// The records this producer staged, created by its freeze; a producer that stands down keeps them for the
    /// merge. Handed over to the session when the producer finishes.
    AdaptivePartitionBuffersPtr partitions;

    /// The current block's misses, one entry per delayed record, in staging order: the source row, the routing hash,
    /// the run length of a count record, and the key, as its size for a string-like key or its value for a
    /// fixed-width one (`miss_keys`, the raw bytes of one key per record).
    PaddedPODArray<UInt32> miss_source_rows;
    PaddedPODArray<UInt64> miss_hashes;
    PaddedPODArray<UInt64> miss_key_sizes;
    PaddedPODArray<char> miss_keys;
    PaddedPODArray<UInt32> miss_multiplicities;

    /// The producer's count bins when the session prunes (see `AdaptiveTopKPruning`), created by its freeze, or by its
    /// finish if it never froze, and handed over at the finish.
    std::unique_ptr<UInt16[]> count_bins;
};

}
