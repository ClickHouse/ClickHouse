#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
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
/// A learning table slices its blocks so that it freezes exactly at the key-count threshold (see
/// `Aggregator::executeImplUntilAdaptiveFreeze`), until it has taken more than this many times the threshold in rows.
/// A table still below the threshold by then gains on average fewer than one key per this many rows, so a block of
/// 65,536 rows that crosses it overshoots by about 4,096 keys, as many as `adaptive_learning_slice_rows` already
/// allows. Its blocks are then aggregated whole, and the check between blocks still freezes it. A table that never
/// reaches the threshold, such as one of few groups or one held to its K best keys by the `GROUP BY` top-K, would
/// otherwise slice every block, and the extra calls cost a few percent of the aggregation.
constexpr size_t adaptive_learning_slicing_row_multiple = 16;
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
/// A batch of misses that keeps the bytes of its keys (see `AdaptiveMissBatch`) is staged once it holds this many of
/// them, in the middle of the block if need be. The serialized methods serialize row by row when the keys of a block are
/// large, so the kept keys of a whole block could take as much memory as its records. Bounded, they are a small scratch
/// per producer, which the writer reads back from the cache.
constexpr size_t adaptive_miss_batch_max_key_bytes = 256 << 10;
/// Fixed lookahead of the drain's hash prefetch.
constexpr size_t adaptive_drain_prefetch_look_ahead = 16;
/// How much of the next range of records the drain requests ahead of its prefetch cursor: a page, the most a
/// hardware stream prefetcher covers without a fresh start.
constexpr size_t adaptive_drain_range_prefetch_bytes = 4096;
/// A merge unit's table is prefetched into only when it is larger than this: below it the table stays in the
/// cache through the drain, and a prefetch of a cache-resident slot is pure overhead.
constexpr size_t adaptive_drain_prefetch_min_table_bytes = 512 << 10;
/// A producer over the external-aggregation threshold writes its staged records to disk only once they take this much
/// memory, its learning table only once the table holds this much, and its frozen table only once the table's states
/// grew by this much since its freeze (see `Aggregator::adaptiveTableWorthSpilling`). A smaller threshold lowers the
/// floor to half of the threshold split among the threads. A write of staged records takes a block and a lock per
/// partition, and a written table learns its keys again from empty, so a producer that just wrote must not write a
/// trickle of new bytes on every block that follows.
constexpr size_t adaptive_spill_min_bytes = 16 << 20;
/// The spill streams of a session, one per partition, are open together, and each holds about three buffers of the
/// streams' buffer size: the compression input, the compressed block and the file's write buffer. Their buffer size
/// therefore follows the external-aggregation threshold, so that the streams together take at most an eighth of it,
/// but not below this floor, under which the writes and the compressed blocks get too small, and not above the buffer
/// size configured for temporary files. Without a threshold they take the floor.
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
/// narrow fixed-width records pays ~ 24 bytes per repeat (a numeric key, a small argument and the cost per
/// record), so it crosses the bound only past repeat ~ 13, where the pathological mid-cardinality streams live.
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

/// The thaw weighs every staged record at its bytes plus this fixed cost, for the lookup of its group that the merge
/// pays per record, where an ordinary table updates the state in place. Its value is the cost per record that the
/// bounds above are calibrated with.
constexpr size_t adaptive_thaw_record_cost = 8;

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
        , spilled_partitions(layout_.numPartitions())
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
    /// `AdaptiveTopKPruning`). The producers hand their count bins, or their tables if they never froze, over to it,
    /// and after the finish barrier `Aggregator::prepareAdaptiveTopKPruning` keeps it only for a merge that can prune.
    std::unique_ptr<AdaptiveTopKPruning> top_k_pruning;

    /// The producers' staged records, handed over by each producer when it finishes (see
    /// `Aggregator::finishAdaptiveProducer`). While other producers still run, the memory scheduler may write them to
    /// `spilled_partitions` under the mutex (see `Aggregator::spillHandedOverStagedRecords`), until the last producer
    /// ends those spills at the finish barrier (see `endHandedOverSpills`). The merge tasks read them without the
    /// mutex: they are created after the finish barrier, which ordered every hand-over and spill before them.
    std::mutex producer_buffers_mutex;
    std::vector<std::unique_ptr<PartitionedRecordBuffer>> producer_buffers;
    /// The memory of the records in `producer_buffers` that a spill can still free, changed under the mutex. Read
    /// without it: the scheduler reads it around every job of a producer, which must not wait for a spill.
    std::atomic<size_t> spillable_handed_over_bytes = 0;

    /// Ends the spills of the records in `producer_buffers` at the finish barrier: waits for a spill in progress, and
    /// zeroes `spillable_handed_over_bytes`, which no hand-over raises again, so the later requests find nothing to
    /// spill.
    void endHandedOverSpills();

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
    /// `Aggregator::spillAdaptivePartitions`): one raw stream per partition, shared by the producers and created by the
    /// first spill into it, so that a merge unit reads back the records of its own partitions only. A stream is a
    /// sequence of blocks, one per spill into the partition, {UInt64 bytes, the records}; the merge task of the
    /// partition's bucket reads it back after the finish barrier, a block at a time.
    class SpilledPartition
    {
    public:
        /// Appends the records of partition `partition` of `partitions` to the stream as one block, creating the stream
        /// in `scope` on the first append, and returns the block's bytes. On a temporary volume the stream is created
        /// on a disk with `reserve_size` bytes of room, and the append throws `NOT_ENOUGH_SPACE` when no disk has them.
        /// Producers append concurrently.
        UInt64 append(
            const TemporaryDataOnDiskScopePtr & scope, const PartitionedRecordBuffer & partitions, size_t partition, size_t reserve_size);

        /// Reads the stream back in the order the producers wrote it and calls `callback(block)` with the records of
        /// each block in a padded buffer like a chunk's. Every block comes in the same buffer, which the next block
        /// overwrites, so the records of a block are valid only during its call. The read stops when `callback`
        /// returns false. The stream stays, so a merge unit that passes over its records twice reads it again; the
        /// merge removes it once the unit no longer needs it. Called by the one task that merges the partition's
        /// bucket, after the finish barrier, so no producer appends any more.
        void readBack(const std::function<bool(const PaddedPODArray<char> &)> & callback);

        /// The records appended to the stream, which the merge counts before it reads them back.
        size_t recordCount() const { return record_count; }

        /// Removes the stream, read back or not.
        void release() { stream.reset(); }

    private:
        /// Serializes the producers' appends and the creation of the stream.
        std::mutex mutex;
        std::unique_ptr<TemporaryDataBuffer> stream;
        size_t record_count = 0;
    };
    /// One per partition of the layout.
    std::vector<SpilledPartition> spilled_partitions;
};

/// One adaptive merge task, which merges a sequence of buckets one at a time (see
/// `Aggregator::mergeAndConvertAdaptiveBucket`). It keeps the bucket it merged last, whose table it takes over for the
/// next one, and its working memory, so the units of its buckets do not allocate it again: the places of a unit's
/// merge and the source slots whose states merge into them, the places, pointers and ranges of a partition's records,
/// and for a count-first unit the hashes of its best groups and the counts an ascending selection keeps.
struct AdaptiveMergeTask
{
    /// The bucket the task merged last, or -1 before its first.
    Int32 previous_bucket = -1;


    PaddedPODArray<AggregateDataPtr> places;
    PaddedPODArray<AggregateDataPtr *> source_slots;
    /// The merges of `source_slots` into `places` ordered by their destination, and one destination's states, for the
    /// pool-parallel merge of a group many producers hold (see `Aggregator::mergeAdaptiveSourceStates`).
    PaddedPODArray<UInt32> merge_order;
    AggregateDataPtrs merge_group;
    PaddedPODArray<AggregateDataPtr> record_places;
    RowStorePointers records;
    AdaptiveRecordRanges ranges;
    std::vector<std::pair<UInt64, UInt64>> best_counts_and_hashes;
    PaddedPODArray<UInt64> best_hashes;
};

/// The misses of a frozen producer's current block, or of a part of it, one descriptor per delayed record in staging
/// order: the routing hash, the source row of the record's arguments, the multiplicity of a count record, and the key.
/// The misses are added in row order, so the source rows ascend. A count record stands for the rows of its key that the
/// kernel combined into it, consecutive or not. A string-like key is kept as its size, and the writer reads its bytes
/// from the key state, which holds them for the whole block. When the state does not, as with the serialized methods
/// that serialize row by row, the kernel keeps the bytes as well, one key after another, and once for all the records
/// of a constant key: building the key again would serialize it a second time. The kernel stages such a batch whenever
/// the kept bytes reach `adaptive_miss_batch_max_key_bytes`. A fixed-width key is kept as its value, which the kernel
/// has at hand, while reading it back from the key columns would pack it a second time. The arrays are cleared after
/// every staging but keep their capacity.
class AdaptiveMissBatch
{
public:
    /// Adds the miss of `row`, with the bytes of a string-like key if `keep_key_bytes`.
    template <typename RecordKey, typename Key>
    ALWAYS_INLINE void add(UInt32 row, UInt64 hash, const Key & key, bool keep_key_bytes)
    {
        chassert(source_rows.empty() || source_rows.back() < row);
        source_rows.push_back(row);
        hashes.push_back(hash);
        addKey<RecordKey>(key, keep_key_bytes);
    }

    /// Adds a count record of the key of `row` with `multiplicity` rows, with the bytes of a string-like key if
    /// `keep_key_bytes`. A fixed-width key stages no size: the writer substitutes the compile-time constant, so the hot
    /// staging loop skips a dead store per record.
    template <typename RecordKey, typename Key>
    ALWAYS_INLINE void addCount(UInt32 row, UInt64 hash, const Key & key, UInt32 multiplicity, bool keep_key_bytes)
    {
        chassert(source_rows.empty() || source_rows.back() < row);
        hashes.push_back(hash);
        multiplicities.push_back(multiplicity);
        addKey<RecordKey>(key, keep_key_bytes);
        source_rows.push_back(row);
    }

    /// Adds a row to count record `i`, or to the last count record.
    ALWAYS_INLINE void incrementCount(size_t i) { ++multiplicities[i]; }
    ALWAYS_INLINE void incrementLastCount() { ++multiplicities.back(); }

    /// Marks the block's keys as constant, before the misses are added: their columns hold a single row, from which every
    /// key is read, while the arguments of each record stay at its source row. The records then share the kept bytes of
    /// the key.
    void setKeysConst()
    {
        chassert(empty());
        keys_are_const = true;
    }
    bool keysAreConst() const { return keys_are_const; }

    size_t size() const { return hashes.size(); }
    bool empty() const { return hashes.empty(); }
    UInt64 hashAt(size_t i) const { return hashes[i]; }
    UInt64 lastHash() const { return hashes.back(); }

    /// The descriptors, read-only, for the writer and the accounting of the batch. The keys are the sizes of string-like
    /// keys with the bytes the kernel kept, one key after another, or the raw bytes of one fixed-width key per record.
    const PaddedPODArray<UInt32> & getSourceRows() const { return source_rows; }
    const PaddedPODArray<UInt64> & getHashes() const { return hashes; }
    const PaddedPODArray<UInt64> & getKeySizes() const { return key_sizes; }
    const PaddedPODArray<char> & getKeys() const { return keys; }
    const PaddedPODArray<UInt32> & getMultiplicities() const { return multiplicities; }

    void clear()
    {
        source_rows.clear();
        hashes.clear();
        key_sizes.clear();
        keys.clear();
        multiplicities.clear();
        keys_are_const = false;
    }

private:
    template <typename RecordKey, typename Key>
    ALWAYS_INLINE void addKey(const Key & key, [[maybe_unused]] bool keep_key_bytes)
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
        {
            const auto bytes = static_cast<std::string_view>(key);
            /// The records of a constant key share the bytes kept for the first of them.
            if (keep_key_bytes && (!keys_are_const || key_sizes.empty()))
                keys.insert(bytes.data(), bytes.data() + bytes.size());
            key_sizes.push_back(bytes.size());
        }
        else
        {
            const RecordKey staged = key;
            const char * bytes = reinterpret_cast<const char *>(&staged);
            keys.insert(bytes, bytes + sizeof(staged));
        }
    }

    PaddedPODArray<UInt32> source_rows;
    PaddedPODArray<UInt64> hashes;
    PaddedPODArray<UInt64> key_sizes;
    PaddedPODArray<char> keys;
    PaddedPODArray<UInt32> multiplicities;
    bool keys_are_const = false;
};

/// What `Aggregator::appendDelayedRecords` staged for a batch, for the accounting of the batch (see
/// `Aggregator::accountStagedBatch`): the bytes of the keys, and the bytes of the whole records as the buffer holds them.
struct AdaptiveStagedBytes
{
    size_t key_bytes = 0;
    size_t record_bytes = 0;
};

/// Per-transform context of the adaptive aggregation: the thread's lifecycle phase and its
/// phase-owned counters, its staged records, and the current block's misses.
struct AdaptiveAggregationProducer
{
    explicit AdaptiveAggregationProducer(AdaptiveAggregationSessionPtr shared_) : session(std::move(shared_)) { }

    /// The thread starts learning: the local table inserts as usual while the freeze rule
    /// watches its growth (see `executeOnBlock`). A frozen table that was written to disk
    /// under memory pressure learns again from empty.
    struct LearningState
    {
        /// Rows that the table took while learning, which end the slicing of its blocks (see
        /// `adaptive_learning_slicing_row_multiple`).
        size_t rows = 0;
    };

    /// The adaptive phase proper: the local table only updates the keys it already holds
    /// and misses are staged for the merge. Carries the post-freeze hit-rate
    /// sampling: when the frozen table turns out to hold almost none of the stream's keys
    /// (a uniform high-cardinality distribution), probing it is pure overhead on every row;
    /// after the sample window the kernel switches to staging ordinary blocks without the lookup.
    /// Constant-key blocks still look up their one key, so they can keep growing the retained states
    /// even after per-row probes are bypassed.
    struct FrozenState
    {
        FrozenState(size_t used_arena_bytes_at_freeze_, size_t memory_usage_at_freeze_, size_t estimated_table_bytes_per_key_)
            : used_arena_bytes_at_freeze(used_arena_bytes_at_freeze_)
            , memory_usage_at_freeze(memory_usage_at_freeze_)
            , estimated_table_bytes_per_key(estimated_table_bytes_per_key_)
        {
        }

        /// Bytes that the table's keys and states took in its arenas at the freeze (see
        /// `AggregatedDataVariants::usedArenaBytes`), and the table's tracked memory then, which also counts the heap
        /// memory that its states own (see `AggregatedDataVariants::memoryUsage`). The frozen-table spill guard
        /// measures the growth of the states against both.
        size_t used_arena_bytes_at_freeze;
        size_t memory_usage_at_freeze;
        /// Estimated hash-table buffer and arena bytes per key. A populated table supplies its measured
        /// allocations, including capacity and allocation overhead. An empty table uses the aggregate state
        /// size as a lower bound. This excludes staged records, other workers' memory and allocations owned
        /// directly by aggregate states, matching `AggregatedDataVariants::allocatedBytes`.
        size_t estimated_table_bytes_per_key;
        size_t sampled_hits = 0;
        size_t sampled_rows = 0;
        bool bypass_local_probe = false;

        /// Records the rows represented by local lookups and the hits among them, and returns whether per-row probing
        /// was just bypassed. Nothing is recorded once it is.
        bool recordLocalProbeResults(size_t hits, size_t probe_rows)
        {
            if (bypass_local_probe)
                return false;
            sampled_hits += hits;
            sampled_rows += probe_rows;
            if (sampled_rows >= adaptive_bypass_sample_rows
                && sampled_hits * adaptive_bypass_hit_rate_inverse < sampled_rows)
            {
                bypass_local_probe = true;
                return true;
            }
            return false;
        }

        /// The thaw evidence of this thread (see `Aggregator::adaptiveStagingRepeats`): the rows the frozen table saw,
        /// the records it staged and their cost (see `Aggregator::accountStagedBatch`), and a sparse sample of distinct
        /// staged hashes. Scaling the sample estimates the number of keys, which the known record count turns into a
        /// repetition estimate.
        size_t rows = 0;
        size_t staged_records = 0;
        size_t staged_bytes = 0;
        HashSet<UInt64> distinct_sampled_hashes;

        size_t getEstimatedStagedKeyCount() const
        {
            /// Staged keys that all fall outside the sample number fewer than about one sample's worth, so an empty
            /// sample counts as one sampled key.
            return std::max<size_t>(distinct_sampled_hashes.size(), 1) * (adaptive_thaw_sample_mask + 1);
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

    void freeze(size_t used_arena_bytes_at_freeze, size_t memory_usage_at_freeze, size_t estimated_table_bytes_per_key)
    {
        phase.emplace<FrozenState>(used_arena_bytes_at_freeze, memory_usage_at_freeze, estimated_table_bytes_per_key);
    }
    void learnAgain() { phase = LearningState{}; }
    void standDown() { phase = BaselineState{}; }

    AdaptiveAggregationSessionPtr session;

    /// The records this producer staged, created by its freeze; a producer that stands down keeps them for the
    /// merge. Handed over to the session when the producer finishes.
    std::unique_ptr<PartitionedRecordBuffer> partitions;
    /// Memory that the staged records in `partitions` take.
    size_t stagedBytes() const { return partitions ? partitions->allocatedChunkBytes() : 0; }
    /// The producer's staged records, including those already written to disk, for the merge-work estimate.
    size_t total_staged_records = 0;

    /// The current block's misses.
    AdaptiveMissBatch misses;

    /// The producer's count bins when the aggregation prunes by the top-K, allocated by the first freeze and handed
    /// over at the finish.
    AdaptiveTopKCountBins count_bins;
};

}
