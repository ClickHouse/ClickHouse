#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <type_traits>
#include <variant>

#include <AggregateFunctions/IAggregateFunction_fwd.h>

#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Common/HashTable/HashSet.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/ThreadPool_fwd.h>

#include <QueryPipeline/SizeLimits.h>

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <Parsers/IAST_fwd.h>

#include <Interpreters/AggregatedData.h>
#include <Interpreters/AggregatedDataVariants.h>
#include <Interpreters/AggregationMethod.h>
#include <Interpreters/HashTablesStatistics.h>

namespace DB
{

class Arena;
using ArenaPtr = std::shared_ptr<Arena>;
using Arenas = std::vector<ArenaPtr>;

using ColumnsHashing::HashMethodContext;
using ColumnsHashing::HashMethodContextPtr;
using ColumnsHashing::LastElementCacheStats;

class CompiledAggregateFunctionsHolder;
class NativeWriter;
struct OutputBlockColumns;

struct GroupingSetsParams
{
    GroupingSetsParams() = default;

    GroupingSetsParams(Names used_keys_, Names missing_keys_) : used_keys(std::move(used_keys_)), missing_keys(std::move(missing_keys_)) { }

    Names used_keys;
    Names missing_keys;
};

using GroupingSetsParamsList = std::vector<GroupingSetsParams>;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

/// Shared state of the adaptive aggregation.
/// One instance per aggregation, created by `AggregatingStep` when the query qualifies,
/// owned by `ManyAggregatedData` and shared by all its transforms.
///
/// Production phase: every thread aggregates into its own local hash table as usual, until the
/// table holds `adaptive_aggregator_freeze_threshold` keys and freezes. From that point a row
/// whose key the table already holds (a frequent key, learned for free from the first rows)
/// keeps aggregating in place with zero coordination, while a miss (a rare key) is not inserted
/// anywhere: it becomes a delayed record in one of the 256 backlogs, chosen by the two-level
/// bucket of the key's hash. A record is the key value itself with a run-length count when the
/// only aggregate is count, and otherwise the key plus its row's aggregate-argument values,
/// gathered into dense per-block columns at publish so the source block is released; both
/// carry the precomputed routing hash. Nothing is drained while production runs unless memory
/// demands it: past the external-aggregation threshold a pressure sweep drains the backlogs
/// early into the shared routing table and, if that is not enough, spills the routing table
/// through the ordinary external-aggregation machinery, so the memory bound holds.
///
/// Two guards hand the work back to the baseline path, with its ordinary byte-triggered
/// two-level conversion, when freezing cannot pay. A table that consumes many times the
/// threshold in rows while staying below it in keys gives up on freezing, per thread: the
/// stream has few groups (typically with fat states, which want the conversion and its
/// bucket-parallel merge). And when the staged stream as a whole proves to repeat the same keys
/// over and over, every thread thaws its table: those repeats are neither frequent enough for
/// the local tables nor rare keys to store once, and staging them re-processes the bulk of the
/// stream that ordinary insertion would absorb as cheap in-place updates. The thaw verdict is
/// remembered in the hash-table statistics, so later runs of the query skip the engagement
/// altogether instead of re-measuring the stream.
///
/// Merge phase: at the end of input every local table converts to two-level and the standard
/// bucket-parallel merge runs, except that the merge task owning bucket b first drains backlog b
/// into the destination's bucket b (it is the exclusive owner, so no locks are needed) and only
/// then folds the locals' bucket b in as usual.
///
/// The net effect: frequent keys stay in small cache-resident tables, and a rare key is stored
/// and emplaced exactly once, by one thread, instead of once per thread that saw it.
struct StagedChunkPreparation;

/// Who owns a staged key once it is emplaced into a table: the merge-time drain borrows the
/// chunk's bytes (the chunks are retained until after the conversion), while a pressure-time
/// drain copies them into the bucket's arena, because freeing the chunks is its purpose.
enum class AdaptiveKeyStorage
{
    BorrowFromChunk,
    CopyToArena,
};

/// Why an early drain runs: the memory valve stops at its watermark and re-enqueues the rest,
/// the finish path takes everything to put the backlogs into disk-mergeable form.
enum class AdaptiveDrainGoal
{
    UntilLowWatermark,
    All,
};

struct AdaptiveAggregationSession
{
    static constexpr size_t NUM_BUCKETS = 256;

    /// All delayed records of one consumed block, grouped by bucket: bucket b owns the slice
    /// [bucket_offsets[b], bucket_offsets[b + 1]) of every per-record array, and `routing_hashes` (the
    /// routing hash, reused by the drain's emplace) is filled for every record. The payload is
    /// one of two modes:
    ///  - value-staged (`value_staged`, simple-count aggregation only): the record is the key
    ///    itself plus a run-length count, with no argument columns;
    ///  - row-reference (general aggregates): record j reads its aggregate arguments from row j
    ///    of `argument_columns`, which hold the records' values gathered at publish in the same
    ///    bucket-grouped order, so a bucket's slice is a contiguous row range (sparse arguments
    ///    are materialized by the gather, so the staged columns are always dense); the key bytes
    ///    are staged as well, so the drain emplaces without constructing a hashing state per
    ///    (block, bucket) slice.
    ///
    /// One record batch per consumed block, rather than one per (block, bucket); a thread's
    /// small batches are further coalesced into one larger chunk of the same shape before they
    /// reach the backlogs.
    struct StagedChunk
    {
        /// The row-reference mode's argument columns, compacted at publish (see above): only
        /// the aggregate-argument positions are filled, kept at their original indexes so that
        /// the instruction preparation can index the vector.
        Columns argument_columns;
        PaddedPODArray<UInt64> routing_hashes;
        std::array<UInt32, NUM_BUCKETS + 1> bucket_offsets{};

        bool value_staged = false;
        /// The key of the i-th record occupies `key_bytes[key_offsets[i], key_offsets[i + 1])`,
        /// in the same bucket-grouped order as `routing_hashes`; `multiplicities[i]` is the run length
        /// (consecutive occurrences of one key collapse into one record at staging time).
        PaddedPODArray<char> key_bytes;
        PaddedPODArray<UInt64> key_offsets;
        PaddedPODArray<UInt32> multiplicities;

        /// Built by the producer when the chunk is published (see `enqueueStagedChunk`), so a
        /// published chunk is immutable and the drains read it without coordination. Never
        /// built for value-staged blocks.
        std::unique_ptr<const StagedChunkPreparation> prepared;

        ~StagedChunk();
    };
    using StagedChunkPtr = std::shared_ptr<StagedChunk>;

    /// TODO (nihalzp): Consider using a lock-free queue for the backlog, to avoid contention on the mutex.
    struct Bucket
    {
        /// Guards the backlog list against concurrent appends, and against the swap-out of a
        /// pressure sweep. The merge task that owns the bucket consumes what is left without
        /// the mutex: by then production is over and the blocks stay put, because the emplaced
        /// keys point into their staged bytes.
        std::mutex backlog_mutex;
        /// Chunks holding a non-empty slice for this bucket.
        std::vector<StagedChunkPtr> backlog;
    };

    std::array<Bucket, NUM_BUCKETS> buckets;

    /// An empty two-level variant of the query's aggregation method, initialized by the first
    /// thread that freezes. Under memory pressure the production-time sweeps drain staged
    /// records into it early (see `pressureDrainStagedBlocks`); it joins the merge set when it
    /// holds data.
    AggregatedDataVariantsPtr early_drain_variants;

    /// Serializes pressure sweeps: one sweeper at a time sheds memory, and a single sweeper
    /// needs no per-bucket coordination; merge-time drains run after the finish barrier and
    /// need none either. Producers over the trigger block on it deliberately - pausing
    /// production is the backpressure that lets the sweep win.
    std::mutex pressure_sweep_mutex;
    /// Makes a batch's registration in the per-bucket backlogs atomic against a sweep's
    /// collection: a sweep that caught a half-registered chunk would drain all of its buckets
    /// chunk-major, and the producer would then re-register the rest for a second, double-
    /// counting drain at merge time. Producers share the lock (per-bucket mutexes still order
    /// their pushes); only a collecting sweep takes it exclusively.
    std::shared_mutex backlog_registry_mutex;
    /// Whether any early drain moved records into `early_drain_variants`: the finish path then
    /// includes it in the merge set.
    std::atomic<bool> early_drain_started{false};
    std::once_flag init_flag;
    std::atomic<bool> initialized{false};

    /// Records currently enqueued and not yet drained: publishes add, drains subtract their
    /// actual progress, and the seal subtracts what its deduplication merges away. Read for
    /// logging at the finish, after every producer flushed.
    std::atomic<size_t> undrained_records{0};

    /// The thaw sampler (see the tuning constants in `Aggregator.cpp`). At publish the threads
    /// fold a sparse sample of their staged record hashes in here; repeats of a key collapse
    /// onto one entry across all threads, so sampled records per distinct sampled hash estimates
    /// the repeat factor of the staged stream as a whole, independently of how a key's
    /// occurrences spread over the threads.
    std::mutex thaw_sample_mutex;
    HashSet<UInt64> distinct_sampled_hashes;
    size_t thaw_sampled_records = 0;
    size_t staged_records = 0;
    /// Set once the staged stream proves repeat-dominated; every thread then thaws its local
    /// table at the next block and returns to the baseline path for good.
    std::atomic<bool> thaw_all{false};
};

using AdaptiveAggregationSessionPtr = std::shared_ptr<AdaptiveAggregationSession>;

/** How are "total" values calculated with WITH TOTALS?
  * (For more details, see TotalsHavingTransform.)
  *
  * In the absence of group_by_overflow_mode = 'any', the data is aggregated as usual, but the states of the aggregate functions are not finalized.
  * Later, the aggregate function states for all rows (passed through HAVING) are merged into one - this will be TOTALS.
  *
  * If there is group_by_overflow_mode = 'any', the data is aggregated as usual, except for the keys that did not fit in max_rows_to_group_by.
  * For these keys, the data is aggregated into one additional row - see below under the names `overflow_row`, `overflows`...
  * Later, the aggregate function states for all rows (passed through HAVING) are merged into one,
  *  also overflow_row is added or not added (depending on the totals_mode setting) also - this will be TOTALS.
  */


/** Aggregates the source of the blocks.
  */
class Aggregator final
{
public:
    using AggregateFunctionContainer = PaddedPODArray<AggregateDataPtr>;
    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateColumnsData = std::vector<AggregateFunctionContainer *>;
    using AggregateColumnsConstData = std::vector<const AggregateFunctionContainer *>;
    using AggregateFunctionsPlainPtrs = std::vector<const IAggregateFunction *>;

    /// Result of aggregation: columns with metadata, without Block overhead.
    struct AggregatedChunk
    {
        Chunk chunk;
        Int32 bucket_num = -1;
        bool is_overflows = false;
    };
    using AggregatedChunks = std::list<AggregatedChunk>;

    struct Params
    {
        /// What to count.
        Names keys;
        size_t keys_size = 0;
        const AggregateDescriptions aggregates;
        const size_t aggregates_size = 0;

        ///
        /// The settings of approximate calculation of GROUP BY.
        ///
        /// Do we need to put into AggregatedDataVariants::without_key aggregates for keys that are not in max_rows_to_group_by.
        const bool overflow_row = false;
        const size_t max_rows_to_group_by = 0;
        const OverflowMode group_by_overflow_mode = OverflowMode::THROW;

        /// Two-level aggregation settings (used for a large number of keys).
        /// With how many keys or the size of the aggregation state in bytes,
        /// two-level aggregation begins to be used. Enough to reach of at least one of the thresholds.
        /// 0 - the corresponding threshold is not specified.
        size_t group_by_two_level_threshold = 0;
        size_t group_by_two_level_threshold_bytes = 0;

        /// Settings to flush temporary data to the filesystem (external aggregation).
        /// 0 - do not use external aggregation.
        size_t max_bytes_before_external_group_by = 0;
        /// Return empty result when aggregating without keys on empty set.
        bool empty_result_for_aggregation_by_empty_set = false;
        TemporaryDataOnDiskScopePtr tmp_data_scope;
        size_t max_threads = 0;
        const size_t min_free_disk_space = 0;
        bool compile_aggregate_expressions = false;
        size_t min_count_to_compile_aggregate_expression = 0;
        size_t max_block_size = 0;
        bool only_merge = false;
        bool enable_prefetch = false;
        bool optimize_group_by_constant_keys = false;
        const float min_hit_rate_to_use_consecutive_keys_optimization = 0.;
        StatsCollectingParams stats_collecting_params;

        bool enable_adaptive_aggregator = false;
        UInt64 adaptive_aggregator_freeze_threshold = 0;

        bool enable_producing_buckets_out_of_order_in_aggregation = true;

        bool serialize_string_with_zero_byte = false;

        /// Set for aggregation in order (`AggregatingInOrderTransform`). In that mode a fresh
        /// aggregation-method state is constructed for every contiguous run of equal order-key
        /// values (via `executeOnBlockSmall` / `mergeOnBlockSmall`), so a method whose state
        /// construction does work proportional to the whole block turns a single block into
        /// O(number_of_runs * block_size) work. This is the case for the `prealloc_serialized`
        /// method, which serializes all of the block's keys up front on construction. In that mode
        /// the per-run path falls back to the plain `serialized` method (lazy, per-row key
        /// serialization) to keep it linear; see `Aggregator::method_chosen_for_in_order`. The
        /// whole-block merge stage (`mergeBlocks` in `MergingAggregatedBucketTransform`) keeps
        /// `prealloc_serialized`, where it is a win.
        bool aggregation_in_order = false;

        static size_t getMaxBytesBeforeExternalGroupBy(size_t max_bytes_before_external_group_by, double max_bytes_ratio_before_external_group_by);

        Params(
            const Names & keys_,
            const AggregateDescriptions & aggregates_,
            bool overflow_row_,
            size_t max_rows_to_group_by_,
            OverflowMode group_by_overflow_mode_,
            size_t group_by_two_level_threshold_,
            size_t group_by_two_level_threshold_bytes_,
            size_t max_bytes_before_external_group_by_,
            bool empty_result_for_aggregation_by_empty_set_,
            TemporaryDataOnDiskScopePtr tmp_data_scope_,
            size_t max_threads_,
            size_t min_free_disk_space_,
            bool compile_aggregate_expressions_,
            size_t min_count_to_compile_aggregate_expression_,
            size_t max_block_size_,
            bool enable_prefetch_,
            bool only_merge_, // true for projections
            bool optimize_group_by_constant_keys_,
            float min_hit_rate_to_use_consecutive_keys_optimization_,
            const StatsCollectingParams & stats_collecting_params_,
            bool enable_producing_buckets_out_of_order_in_aggregation_,
            bool serialize_string_with_zero_byte_,
            bool enable_adaptive_aggregator_,
            UInt64 adaptive_aggregator_freeze_threshold_);

        /// Only parameters that matter during merge.
        Params(
            const Names & keys_,
            const AggregateDescriptions & aggregates_,
            bool overflow_row_,
            size_t max_threads_,
            size_t max_block_size_,
            float min_hit_rate_to_use_consecutive_keys_optimization_,
            bool serialize_string_with_zero_byte_);

        Params cloneWithKeys(const Names & keys_, bool only_merge_ = false) const
        {
            Params new_params = *this;
            new_params.keys = keys_;
            new_params.keys_size = keys_.size();
            new_params.only_merge = only_merge_;
            return new_params;
        }

        static Block
        getHeader(const Block & header, bool only_merge, const Names & keys, const AggregateDescriptions & aggregates, bool final);

        Block getHeader(const Block & header_, bool final) const { return getHeader(header_, only_merge, keys, aggregates, final); }

        /// Returns keys and aggregated for EXPLAIN query
        void explain(ExplainFormatSettings & settings) const;
        void explain(JSONBuilder::JSONMap & map) const;
    };

    explicit Aggregator(const Block & header_, const Params & params_);
    ~Aggregator();

    const Params & getParams() const { return params; }

    /// Per-transform context of the adaptive aggregation: the thread's lifecycle phase and its
    /// phase-owned counters, per-block staging for the missed rows (the arrays are cleared but
    /// keep their capacity across blocks), and the buffered chunks awaiting coalescing.
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
        /// and misses are staged for the shared drain. Carries the post-freeze hit-rate
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
                /// repeat-dominated (see `publishDelayedRecords`).
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

        /// The current block's misses, one entry per delayed record, in staging order.
        PaddedPODArray<UInt32> miss_source_rows;
        PaddedPODArray<UInt64> miss_hashes;
        PaddedPODArray<UInt8> miss_buckets;
        PaddedPODArray<UInt64> miss_key_sizes;
        PaddedPODArray<UInt32> miss_multiplicities;

        /// Scratch for the value-staged publish grouping (see `buildDeduplicatedCountChunk`).
        std::vector<std::pair<UInt64, UInt32>> sort_pairs_scratch;
        std::vector<UInt32> group_offsets_scratch;
        std::vector<UInt32> group_cursor_scratch;

        /// Small per-block staging batches buffered for coalescing: they are merged into one
        /// bucket-grouped chunk before they reach the backlogs (see `stageChunk`), so the
        /// merge-time drain gets a few large contiguous slices per bucket instead of one tiny
        /// slice per consumed block. Flushed by `flushPendingChunks` when the input ends.
        std::vector<AdaptiveAggregationSession::StagedChunkPtr> pending_chunks;
        size_t pending_staged_bytes = 0;
    };

    /// Process one block. Return false if the processing should be aborted (with group_by_overflow_mode = 'break').
    /// `adaptive` is the per-thread adaptive-aggregation context, or nullptr when the feature is off.
    bool executeOnBlock(Columns columns,
        size_t row_begin, size_t row_end,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys,
        AdaptiveAggregationProducer * adaptive) const;

    /// Drains one bucket's whole backlog into the destination variant's two-level bucket. Called
    /// by the merge task that owns the bucket, before it merges that bucket: production is over
    /// by then and the ownership is exclusive, so no locking beyond the backlog swap is needed.
    void drainAdaptiveBucketForMerge(
        AggregatedDataVariants & dest,
        Arena * arena,
        size_t bucket,
        AdaptiveAggregationSession & shared,
        std::atomic<bool> & is_cancelled) const;

    /// Seals and enqueues this thread's buffered staged blocks. Every producing transform calls
    /// it when its input ends, before the finish barrier, so the backlogs are complete by the
    /// time the last finisher assembles the merge.
    void flushPendingChunks(AdaptiveAggregationProducer & adaptive) const;

    /// Swaps the enqueued chunks out of the backlogs and drains them chunk-major into
    /// `early_drain_variants` (all of one chunk's bucket slices, then the next), so each chunk
    /// frees the moment it is consumed. Runs on at most one thread at a time. The memory-
    /// pressure valve calls it with a watermark: it stops once memory falls below it and
    /// re-enqueues what it did not drain; whenever the routing table reaches the spill floor
    /// while memory is still over the trigger, it spills mid-drain, so the transfer itself
    /// cannot peak above the trigger. The finish path calls it with `AdaptiveDrainGoal::All`
    /// to put the backlogs into disk-mergeable form when a producer has spilled.
    void drainStagedChunksEarly(AdaptiveAggregationSession & shared, AdaptiveDrainGoal goal) const;

    /** This array serves two purposes.
      *
      * Function arguments are collected side by side, and they do not need to be collected from different places. Also the array is made zero-terminated.
      * The inner loop (for the case without_key) is almost twice as compact; performance gain of about 30%.
      */
    struct AggregateFunctionInstruction
    {
        const IAggregateFunction * that{};
        size_t state_offset{};
        const IColumn ** arguments{};
        const IAggregateFunction * batch_that{};
        const IColumn ** batch_arguments{};
        const UInt64 * offsets{};
        bool has_sparse_arguments = false;
        bool can_optimize_equal_keys_ranges = true;
    };

    /// Used for optimize_aggregation_in_order:
    /// - No two-level aggregation
    /// - No external aggregation
    /// - No without_key support (it is implemented using executeOnIntervalWithoutKey())
    void executeOnBlockSmall(
        AggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions) const;

    void executeOnIntervalWithoutKey(
        AggregatedDataVariants & data_variants,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions) const;

    /// Used for aggregate projection.
    bool mergeOnBlock(Columns columns, size_t rows, bool is_overflows,
        AggregatedDataVariants & result,
        bool & no_more_keys,
        std::atomic<bool> & is_cancelled) const;

    void mergeOnBlockSmall(
        AggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        const AggregateColumnsConstData & aggregate_columns_data,
        const ColumnRawPtrs & key_columns) const;

    void mergeOnIntervalWithoutKey(
        AggregatedDataVariants & data_variants,
        size_t row_begin,
        size_t row_end,
        const AggregateColumnsConstData & aggregate_columns_data,
        std::atomic<bool> & is_cancelled) const;

    /** Convert the aggregation data structure into a block.
      * If overflow_row = true, then aggregates for rows that are not included in max_rows_to_group_by are put in the first block.
      *
      * If final = false, then ColumnAggregateFunction is created as the aggregation columns with the state of the calculations,
      *  which can then be combined with other states (for distributed query processing).
      * If final = true, then columns with ready values are created as aggregate columns.
      */
    AggregatedChunks convertToChunks(AggregatedDataVariants & data_variants, bool final) const;

    /// `adaptive_session` (or nullptr when the adaptive aggregation is off) feeds the
    /// thaw verdict into the hash-table statistics next to the observed sizes.
    ManyAggregatedDataVariants prepareVariantsToMerge(
        ManyAggregatedDataVariants && data_variants, AdaptiveAggregationSession * adaptive_session) const;

    using BucketToChunks = std::map<Int32, AggregatedChunks>;
    /// Merge partially aggregated chunks separated to buckets into one data structure.
    void mergeBlocks(BucketToChunks bucket_to_chunks, AggregatedDataVariants & result, std::atomic<bool> & is_cancelled);

    /// Merge several partially aggregated chunks into one.
    /// Precondition: for all chunks the is_overflows flag must be the same.
    /// (either all chunks are from overflow data or none are).
    AggregatedChunk mergeBlocks(
        AggregatedChunks & chunks,
        bool final,
        std::atomic<bool> & is_cancelled,
        const RuntimeDataflowStatisticsCacheUpdaterPtr & dataflow_cache_updater);

    /** Split block with partially-aggregated data to many blocks, as if two-level method of aggregation was used.
      * This is needed to simplify merging of that data with other results, that are already two-level.
      */
    std::vector<AggregatedChunk> convertBlockToTwoLevel(const Columns & columns, size_t rows) const;

    /// For external aggregation.
    void writeToTemporaryFile(AggregatedDataVariants & data_variants, size_t max_temp_file_size = 0) const;

    bool hasTemporaryData() const;

    std::list<TemporaryBlockStreamHolder> detachTemporaryData();

    /// Part of automatic parallel replicas implementation.
    size_t estimateSizeOfCompressedState(AggregatedDataVariants & result, ssize_t bucket) const;

    const ColumnNumbers & getKeysPositions() const { return keys_positions; }
    const DataTypes & getKeyTypes() const { return key_types; }

private:

    friend struct AggregatedDataVariants;
    friend struct StagedChunkPreparation;
    friend class ConvertingAggregatedToChunksTransform;
    friend class ConvertingAggregatedToChunksSource;
    friend class ConvertingAggregatedToChunksWithMergingSource;
    friend class ConvertingAggregatedToChunksWithMergingSourceForFixedHashMap;
    friend class AggregatingInOrderTransform;

    /// Positions of aggregation key columns in the header.
    const ColumnNumbers keys_positions;
    /// Positions of aggregate function argument columns in the header.
    const ColumnNumbersList aggregates_positions;
    /// Types of key columns from the input header.
    const DataTypes key_types;
    /// Types of aggregate function states (DataTypeAggregateFunction), one per aggregate.
    const DataTypes aggregate_state_types;
    Params params;

    AggregatedDataVariants::Type method_chosen;

    /// The aggregation method used by the per-run in-order path (`executeOnBlockSmall` /
    /// `mergeOnBlockSmall`, called only from `AggregatingInOrderTransform`). It equals
    /// `method_chosen`, except that when `Params::aggregation_in_order` is set the `prealloc_serialized`
    /// variants are replaced by their plain `serialized` counterparts. That path builds a fresh state
    /// for every run of equal order-key values, so the up-front whole-block serialization done by
    /// `prealloc_serialized` on construction would make it quadratic. All whole-block paths (including
    /// `mergeBlocks` used by `MergingAggregatedBucketTransform`) keep `method_chosen`, where
    /// `prealloc_serialized` is a win. The `serialized` and `prealloc_serialized` methods produce
    /// byte-identical keys and share the same hash-method context, so mixing them across pipeline
    /// stages is safe.
    AggregatedDataVariants::Type method_chosen_for_in_order;

    Sizes key_sizes;

    HashMethodContextPtr aggregation_state_cache;

    AggregateFunctionsPlainPtrs aggregate_functions;

    using AggregateFunctionInstructions = std::vector<AggregateFunctionInstruction>;
    using NestedColumnsHolder = VectorWithMemoryTracking<VectorWithMemoryTracking<const IColumn *>>;

    Sizes offsets_of_aggregate_states;    /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.

    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;

    bool all_aggregates_has_trivial_destructor = false;

    /// How many RAM were used to process the query before processing the first block. Use for merge_only mode.
    Int64 memory_usage_before_aggregation = 0;
    /// Track memory held by the aggreagation state during execution.
    std::unique_ptr<MemoryTracker> memory_tracker;

    /// Indicates whether the aggregation is a simple `count()` / `count(*)` / `count(non-nullable_column)`
    ///
    /// If true, we can apply an important performance optimization:
    /// - The aggregation logic can be inlined, meaning each row is aggregated immediately during hash table probing.
    /// - There's no need to allocate and maintain full aggregation state.
    bool is_simple_count = false;

    LoggerPtr log = getLogger("Aggregator");

    /// For external aggregation.
    TemporaryDataOnDiskScopePtr tmp_data;
    mutable std::mutex tmp_files_mutex;
    mutable std::list<TemporaryBlockStreamHolder> tmp_files TSA_GUARDED_BY(tmp_files_mutex);

    size_t min_bytes_for_prefetch = 0;

#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledAggregateFunctionsHolder> compiled_aggregate_functions_holder;
#endif

    std::vector<bool> is_aggregate_function_compiled;

    mutable std::unique_ptr<ThreadPool> thread_pool;

    /** Try to compile aggregate functions.
      */
    void compileAggregateFunctionsIfNeeded();

    /** Create states of aggregate functions for one key.
      */
    template <bool skip_compiled_aggregate_functions = false>
    void createAggregateStates(AggregateDataPtr & aggregate_data) const;

    /** Call `destroy` methods for states of aggregate functions.
      * Used in the exception handler for aggregation, since RAII in this case is not applicable.
      */
    void destroyAllAggregateStates(AggregatedDataVariants & result) const;

    void executeImpl(
        AggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys = false,
        bool all_keys_are_const = false,
        AggregateDataPtr overflow_row = nullptr) const;

    /// Process one data block, aggregate the data into a hash table.
    template <typename Method>
    void executeImpl(
        Method & method,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        LastElementCacheStats & consecutive_keys_cache_stats,
        bool no_more_keys,
        bool all_keys_are_const,
        AggregateDataPtr overflow_row) const;

    template <typename Method, typename State>
    void executeImpl(
        Method & method,
        State & state,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        bool all_keys_are_const,
        AggregateDataPtr overflow_row) const;

    /// Specialization for a particular value no_more_keys.
    template <bool prefetch, typename Method, typename State>
    void executeImplBatch(
        Method & method,
        State & state,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        bool all_keys_are_const,
        bool use_compiled_functions,
        AggregateDataPtr overflow_row) const;

    void initAdaptiveSession(AggregatedDataVariants & local_result, AdaptiveAggregationSession & shared) const;

    /// The frozen consume path: rows whose key the local table holds are aggregated in place,
    /// the other rows are staged per bucket and published to the shared backlogs for the
    /// merge-time drain.
    void executeFrozen(
        const Columns & columns,
        size_t row_begin,
        size_t row_end,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        AdaptiveAggregationProducer & adaptive,
        bool all_keys_are_const) const;

    template <typename LocalMethod, typename SharedMethod>
    void executeFrozenImpl(
        LocalMethod & local_method,
        std::type_identity<SharedMethod>,
        Arena * aggregates_pool,
        const Columns & columns,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        AdaptiveAggregationProducer & adaptive,
        bool all_keys_are_const) const;

    /// Groups the current block's staged misses by bucket (counting sort) into one staged chunk
    /// and hands it to `stageChunk`. Key bytes are copied exactly once, straight from
    /// the hashing state's key holder into their bucket position; row-reference mode additionally
    /// gathers the records' aggregate-argument values into dense compacted columns.
    template <typename SharedKey, typename State>
    void publishDelayedRecords(
        const Columns & columns,
        size_t num_rows,
        AdaptiveAggregationProducer & adaptive,
        State & local_find_state,
        Arena & scratch_pool,
        bool value_staged,
        std::optional<UInt32> key_row_override = std::nullopt) const;

    /// Fills a value-staged block with the current misses ordered by (bucket, hash) and merged:
    /// duplicate keys within the block collapse into one record with a summed run length, so a
    /// repeat-heavy staged stream copies each key's bytes once and the drain emplaces it once.
    template <typename SharedKey, typename State>
    void buildDeduplicatedCountChunk(
        const AdaptiveAggregationSession::StagedChunkPtr & block,
        AdaptiveAggregationProducer & adaptive,
        State & local_find_state,
        Arena & scratch_pool,
        std::optional<UInt32> key_row_override) const;

    /// Enqueues one batch for the merge-time drain: a batch of at least half the seal target
    /// goes straight to the backlogs, a small one is buffered, and the buffer is sealed into
    /// one chunk once enough bytes accumulate.
    void stageChunk(
        AdaptiveAggregationProducer & adaptive,
        AdaptiveAggregationSession::StagedChunkPtr block,
        size_t estimated_payload_bytes) const;

    /// Merges the buffered batches into one bucket-grouped chunk of the same shape (bucket b's
    /// records are the concatenation of the batches' b-slices) and enqueues it.
    void sealPendingChunks(AdaptiveAggregationProducer & adaptive) const;

    /// The value-staged variant of the seal merge: keys repeating across the batches collapse
    /// into one record with a summed run length while the records are copied into the chunk.
    void sealValueStagedChunkDeduplicated(
        const std::vector<AdaptiveAggregationSession::StagedChunkPtr> & minis,
        AdaptiveAggregationSession::StagedChunk & chunk) const;

    /// The single publication point: finishes the chunk (builds its preparation in place) and
    /// registers it with every bucket holding a non-empty slice. Chunks are immutable from
    /// here on.
    void enqueueStagedChunk(
        AdaptiveAggregationSession & shared, const AdaptiveAggregationSession::StagedChunkPtr & block) const;

    /// Builds the staged chunk's shared preparation: the aggregate-function instructions over
    /// its argument columns, in the chunk's own stable storage.
    void prepareStagedChunk(AdaptiveAggregationSession::StagedChunk & block) const;

    /// Drains one bucket's backlog into `method.data.impls[bucket_index]`. `key_storage`
    /// selects the ownership: merge-time drains emplace keys pointing into the retained
    /// chunks, while pressure-time drains persist them into the bucket's arena so the chunks
    /// can be freed (the whole point of draining early).
    template <AdaptiveKeyStorage key_storage, typename Method>
    size_t drainAdaptiveBucketBacklog(
        Method & method,
        Arena * arena,
        const std::vector<AdaptiveAggregationSession::StagedChunkPtr> & backlog,
        size_t bucket_index,
        size_t total_records,
        PaddedPODArray<AggregateDataPtr> & places,
        std::atomic<bool> & is_cancelled) const;

    /// Applies one staged chunk's slice [slice_begin, slice_end) to the bucket's table.
    template <AdaptiveKeyStorage key_storage, typename Method>
    void drainAdaptiveBucketImpl(
        Method & method,
        Arena * bucket_arena,
        const AdaptiveAggregationSession::StagedChunk & block,
        size_t slice_begin,
        size_t slice_end,
        PaddedPODArray<AggregateDataPtr> & places,
        size_t bucket_index) const;


    void executeAggregateInstructions(
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        AggregateDataPtr * places,
        size_t key_start,
        bool has_only_one_value_since_last_reset,
        bool all_keys_are_const,
        bool use_compiled_functions) const;

    /// For case when there are no keys (all aggregate into one row).
    void executeWithoutKeyImpl(
        AggregatedDataWithoutKey & res,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        Arena * arena,
        bool use_compiled_functions) const;

    template <typename Method>
    void writeToTemporaryFileImpl(
        AggregatedDataVariants & data_variants,
        Method & method,
        TemporaryBlockStreamHolder & out) const;

    /// Parameters for parallel merge workers for single level.
    struct ParallelMergeWorker
    {
        UInt32 worker_id;
        UInt32 total_worker;
    };

    /// Merge NULL key data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataNullKey(
            Table & table_dst,
            Table & table_src,
            Arena * arena) const;

    /// Merge data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataImpl(
        Table & table_dst, Table & table_src, Arena * arena, bool use_compiled_functions, bool prefetch,
        std::atomic<bool> & is_cancelled, const ParallelMergeWorker * parallel_worker = nullptr)
        const;

    /// Merge data from hash table `src` into `dst`, but only for keys that already exist in dst. In other cases, merge the data into `overflows`.
    template <typename Method, typename Table>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena) const;

    /// Same, but ignores the rest of the keys.
    template <typename Method, typename Table>
    void mergeDataOnlyExistingKeysImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena) const;

    void mergeWithoutKeyDataImpl(
        ManyAggregatedDataVariants & non_empty_data,
        std::atomic<bool> & is_cancelled) const;

    template <typename Method>
    void mergeSingleLevelDataImpl(
        ManyAggregatedDataVariants & non_empty_data, std::atomic<bool> & is_cancelled) const;

    /// Disable min-max optimization for fixed-size hash tables to avoid race conditions.
    void disableMinMaxOptimizationForFixedHashMaps(ManyAggregatedDataVariants & data_variants) const;

    template <typename Method>
    void mergeSingleLevelDataImplFixedMap(
        ManyAggregatedDataVariants & non_empty_data,
        Arena * arena,
        UInt32 worker_id,
        UInt32 total_worker,
        std::atomic<bool> & is_cancelled) const;

    /// Non-template wrapper that handles type switch internally
    void mergeSingleLevelDataImplFixedMap(
        ManyAggregatedDataVariants & non_empty_data,
        Arena * arena,
        UInt32 worker_id,
        UInt32 total_worker,
        std::atomic<bool> & is_cancelled) const;

    /// Set data_variants[1..last] aggregator as nullptr to ensure aggregator destruction only invoked in data_variants[0]'s destructor.
    /// Used for single level merge.
    void resetAggregatorExceptFirst(ManyAggregatedDataVariants & data_variants) const;

    template <typename Method, typename Table>
    Chunks
    convertToBlockImpl(Method & method, Table & data, Arena * arena, Arenas & aggregates_pools, bool final, size_t rows, bool return_single_block) const;

    template <typename Mapped>
    void insertAggregatesIntoColumns(
        Mapped & mapped,
        MutableColumns & final_aggregate_columns,
        Arena * arena) const;

    Chunk insertResultsIntoColumns(
        PaddedPODArray<AggregateDataPtr> & places,
        OutputBlockColumns && out_cols,
        Arena * arena,
        bool has_null_key_data,
        bool use_compiled_functions) const;

    template <typename Method, typename Table>
    Chunks convertToBlockImplFinal(
        Method & method,
        Table & data,
        Arena * arena,
        Arenas & aggregates_pools,
        bool use_compiled_functions,
        bool return_single_block) const;

    template <typename Method, typename Table>
    Chunks
    convertToBlockImplNotFinal(Method & method, Table & data, Arenas & aggregates_pools, size_t rows, bool return_single_block) const;

    template <typename Method>
    AggregatedChunk convertOneBucketToChunk(
        AggregatedDataVariants & data_variants,
        Method & method,
        Arena * arena,
        bool final,
        Int32 bucket) const;

    AggregatedChunk convertOneBucketToChunk(AggregatedDataVariants & variants, Arena * arena, bool final, Int32 bucket) const;

    AggregatedChunk mergeAndConvertOneBucketToChunk(
        ManyAggregatedDataVariants & variants,
        Arena * arena,
        bool final,
        Int32 bucket,
        std::atomic<bool> & is_cancelled,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater) const;

    AggregatedChunk prepareChunkAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const;
    AggregatedChunks prepareChunksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final) const;

    template <bool return_single_block>
    std::conditional_t<return_single_block, AggregatedChunk, AggregatedChunks>
    prepareChunkAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const;

    template <typename Method>
    AggregatedChunks prepareChunksAndFillTwoLevelImpl(AggregatedDataVariants & data_variants, Method & method, bool final) const;

    template <typename State, typename Table>
    void mergeStreamsImplCase(
        Arena * aggregates_pool,
        State & state,
        Table & data,
        bool no_more_keys,
        AggregateDataPtr overflow_row,
        size_t row_begin,
        size_t row_end,
        const AggregateColumnsConstData & aggregate_columns_data,
        std::atomic<bool> & is_cancelled,
        Arena * arena_for_keys) const;

    /// `arena_for_keys` used to store serialized aggregation keys (in methods like `serialized`) to save some space.
    /// If not provided, aggregates_pool is used instead. Refer to mergeBlocks() for an usage example.
    template <typename Method, typename Table>
    void mergeStreamsImpl(
        const Columns & columns,
        size_t rows,
        Arena * aggregates_pool,
        Method & method,
        Table & data,
        AggregateDataPtr overflow_row,
        LastElementCacheStats & consecutive_keys_cache_stats,
        bool no_more_keys,
        std::atomic<bool> & is_cancelled,
        Arena * arena_for_keys = nullptr) const;

    template <typename Method, typename Table>
    void mergeStreamsImpl(
        Arena * aggregates_pool,
        Method & method,
        Table & data,
        AggregateDataPtr overflow_row,
        LastElementCacheStats & consecutive_keys_cache_stats,
        bool no_more_keys,
        size_t row_begin,
        size_t row_end,
        const AggregateColumnsConstData & aggregate_columns_data,
        const ColumnRawPtrs & key_columns,
        std::atomic<bool> & is_cancelled,
        Arena * arena_for_keys) const;

    void mergeBlockWithoutKeyStreamsImpl(
        const Columns & columns,
        size_t rows,
        AggregatedDataVariants & result,
        std::atomic<bool> & is_cancelled) const;

    void mergeWithoutKeyStreamsImpl(
        AggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        const AggregateColumnsConstData & aggregate_columns_data,
        std::atomic<bool> & is_cancelled) const;

    template <typename Method>
    void mergeBucketImpl(
        ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena, std::atomic<bool> & is_cancelled) const;

    template <typename Method>
    void convertBlockToTwoLevelImpl(
        Method & method,
        Arena * pool,
        ColumnRawPtrs & key_columns,
        const Columns & source,
        size_t rows,
        std::vector<AggregatedChunk> & destinations) const;

    template <typename Method, typename Table>
    void destroyImpl(Table & table) const;

    void destroyWithoutKey(
        AggregatedDataVariants & result) const;


    /** Checks constraints on the maximum number of keys for aggregation.
      * If it is exceeded, then, depending on the group_by_overflow_mode, either
      * - throws an exception;
      * - returns false, which means that execution must be aborted;
      * - sets the variable no_more_keys to true.
      */
    bool checkLimits(size_t result_size, bool & no_more_keys) const;

    void ensureLimitsFixedMapMerge(AggregatedDataVariantsPtr data) const;

    /// Check if data variants use fixed-size hash tables (key8/key16) suitable for parallel merge
    /// at single level.
    bool isTypeFixedSize(const ManyAggregatedDataVariants & data_variants) const;

    void prepareAggregateInstructions(
        Columns columns,
        AggregateColumns & aggregate_columns,
        Columns & materialized_columns,
        AggregateFunctionInstructions & instructions,
        NestedColumnsHolder & nested_columns_holder) const;

    void addSingleKeyToAggregateColumns(
        AggregatedDataVariants & data_variants,
        MutableColumns & aggregate_columns) const;

    void addArenasToAggregateColumns(
        const AggregatedDataVariants & data_variants,
        MutableColumns & aggregate_columns) const;

    void createStatesAndFillKeyColumnsWithSingleKey(
        AggregatedDataVariants & data_variants,
        Columns & key_columns, size_t key_row,
        MutableColumns & final_key_columns) const;

    static bool hasSparseArguments(AggregateFunctionInstruction * aggregate_instructions);

    static void addBatch(
        size_t row_begin, size_t row_end,
        AggregateFunctionInstruction * inst,
        AggregateDataPtr * places,
        Arena * arena);

    static void addBatchSinglePlace(
        size_t row_begin, size_t row_end,
        AggregateFunctionInstruction * inst,
        AggregateDataPtr place,
        Arena * arena);
};

/** Get the aggregation variant by its type. */
template <typename Method> Method & getDataVariant(AggregatedDataVariants & variants);

#define M(NAME, IS_TWO_LEVEL) \
    template <> inline decltype(AggregatedDataVariants::NAME)::element_type & getDataVariant<decltype(AggregatedDataVariants::NAME)::element_type>(AggregatedDataVariants & variants) { return *variants.NAME; } /// NOLINT

APPLY_FOR_AGGREGATED_VARIANTS(M)

#undef M

}
