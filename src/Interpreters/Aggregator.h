#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>

#include <AggregateFunctions/IAggregateFunction_fwd.h>

#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/ThreadPool_fwd.h>

#include <QueryPipeline/SizeLimits.h>

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <Parsers/IAST_fwd.h>

#include <Interpreters/AdaptiveAggregation.h>
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

struct StagedChunkPreparation;

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


/// The state representation of simple-count aggregation (a lone `count()`): the mapped value
/// itself is the UInt64 counter, so there is no allocated place to point to.
inline UInt64 & getCountState(AggregateDataPtr __restrict place) /// NOLINT(readability-non-const-parameter)
{
    return *reinterpret_cast<UInt64 *>(place);
}

inline UInt64 & getInlineCountState(AggregateDataPtr & ptr)
{
    return getCountState(reinterpret_cast<AggregateDataPtr>(&ptr));
}

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
        UInt64 adaptive_aggregator_freeze_threshold_bytes = 0;

        /// Bucket-local Top-K of the final conversion, set by the `aggregation_bucket_top_k`
        /// plan optimization (never by users) when the plan proves this aggregation feeds
        /// `ORDER BY <the lone count() output> LIMIT n`: each two-level bucket materializes
        /// only its n best cells by that count. Exact, because a group outside its own
        /// bucket's best n has at least n groups ahead of it globally. Zero disables. Kept
        /// out of the constructor and of the plan serialization deliberately: a deserialized
        /// plan re-runs without the optimization, which is the safe direction.
        size_t bucket_top_k = 0;
        bool bucket_top_k_ascending = false;
        size_t bucket_top_k_count_index = 0;

        bool enable_producing_buckets_out_of_order_in_aggregation = true;

        /// Merge the per-thread single-level hash tables in parallel, partitioned by the key hash,
        /// instead of the serial merge.
        bool enable_parallel_single_level_merge = false;

        bool serialize_string_with_zero_byte = false;

        struct TopKParams
        {
            /// LIMIT values above this never get the optimization; keeps the heap
            /// arithmetic and preallocation trivially safe.
            static constexpr size_t max_k = 100000;

            size_t k = 0;                           /// the query's LIMIT K (heap capacity)
            std::vector<int> directions;            /// per-column ORDER BY directions
            std::vector<int> nulls_directions;      /// per-column NULLS/NaNs directions
            size_t key_columns = 0;                 /// leading GROUP BY columns the heap ranks on
            UInt64 observation_rows = 65536;        /// rows before the pure-overhead freeze check; 0 disables it (see the group_by_top_k_optimization_* settings)
        };
        std::optional<TopKParams> top_k;

        /// Use the `PackedStringRef`-based hash table for a single non-nullable `String` key
        /// (`key_packed_string`); if false, fall back to the legacy `StringHashTable`-based method
        /// (`key_string`). The two methods hash keys differently, so all participants of a
        /// distributed query must agree on this value for two-level bucket exchange to be correct;
        /// that is why it also matters for merge-only aggregators (`convertBlockToTwoLevel`).
        bool enable_packed_string_keys = true;

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
            bool enable_parallel_single_level_merge_,
            bool enable_packed_string_keys_,
            bool enable_adaptive_aggregator_,
            UInt64 adaptive_aggregator_freeze_threshold_,
            UInt64 adaptive_aggregator_freeze_threshold_bytes_);

        /// Only parameters that matter during merge.
        Params(
            const Names & keys_,
            const AggregateDescriptions & aggregates_,
            bool overflow_row_,
            size_t max_threads_,
            size_t max_block_size_,
            float min_hit_rate_to_use_consecutive_keys_optimization_,
            bool serialize_string_with_zero_byte_,
            bool enable_packed_string_keys_);

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

    /// Process one block. Return false if the processing should be aborted (with group_by_overflow_mode = 'break').
    /// `adaptive` is the per-thread adaptive-aggregation context, or nullptr when the feature is off.
    bool executeOnBlock(Columns columns,
        size_t row_begin, size_t row_end,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys,
        AdaptiveAggregationProducer * adaptive) const;

    /// One claimed batch of staged chunks into one drain table, bucket-major: bucket b's
    /// slices from all of the batch's chunks drain consecutively, so the destination subtable
    /// and its arena stay cache-hot across the whole batch instead of being revisited once per
    /// chunk - the measured win of the pressure drains. The price is that the batch stays
    /// alive until the pass ends: the callers bound a batch at about one spill floor of
    /// records and release the chunks right after the call. Stops between buckets when
    /// cancelled.
    size_t drainStagedBatch(
        AggregatedDataVariants & table,
        const std::vector<StagedChunkPtr> & chunks,
        std::atomic<bool> & is_cancelled,
        PaddedPODArray<AggregateDataPtr> & places_scratch) const;

    /// A fresh drain destination of the session's method type, with one arena per bucket.
    AggregatedDataVariantsPtr createAdaptiveDrainTable(AggregatedDataVariants::Type type) const;

    /// Writes a detached drain table through the ordinary external machinery and tears it
    /// down; skipped for a cancelled query, whose table just destroys itself.
    void spillDetachedAdaptiveTable(AdaptiveAggregationSession & shared, AggregatedDataVariants & table) const;

    /// Retires a merged-and-converted bucket's working memory, called by the bucket's merge
    /// task after a successful conversion (the output either copied the values out or captured
    /// the arena slot's ownership): resets the bucket's arena slot and drops the backlog's
    /// chunk references, whose borrow ends at conversion. The destination subtable buffer is
    /// already released by the conversion itself. Never called for a cancelled or failed
    /// bucket - the variants still own every non-retired slot, so ordinary destruction covers
    /// those.
    void retireAdaptiveMergedBucket(AggregatedDataVariants & dest, AdaptiveAggregationSession & shared, size_t bucket) const;

    /// Drains one bucket's whole backlog into the destination variant's two-level bucket. Called
    /// by the merge task that owns the bucket, before it merges that bucket: production finished
    /// before the merge sources were created and the ownership is exclusive, so the backlog is
    /// read in place without locking; the chunks stay registered because the emplaced keys
    /// borrow their staged bytes.
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

    /// The production-time memory valve: claims a bounded batch of staged chunks under the
    /// sweep lock, drains it into a producer-local table outside the lock, and writes that
    /// table through the ordinary external machinery; a sub-floor tail accumulates in the
    /// session's shared table instead. Producers over the trigger block on the claim
    /// deliberately - pausing production is the backpressure that makes the bound hold.
    void drainStagedChunksUnderMemoryPressure(AdaptiveAggregationSession & shared) const;

    /// The finish drain: converts everything still enqueued into disk-mergeable form when the
    /// merge goes external, spilling at the part floor as it goes, and throws if anything
    /// would be left behind.
    void drainStagedChunksAtFinish(AdaptiveAggregationSession & shared) const;

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
    /// Records the thaw verdict in the hash-table statistics when the session measured one.
    /// The in-memory merge records it inside `prepareVariantsToMerge`; the external merge never
    /// reaches that, so the finish path calls this instead.
    void recordAdaptiveStagingVerdict(AdaptiveAggregationSession & shared) const;

    ManyAggregatedDataVariants prepareVariantsToMerge(
        ManyAggregatedDataVariants && data_variants, AdaptiveAggregationSession * adaptive_session) const;

    /// Whether the variants' single-level method can be merged in hash partitions
    /// (`mergeSingleLevelPartitionAndConvertToChunk`): every method with a two-level counterpart, whose
    /// bucket function defines the partition partition.
    bool canMergeSingleLevelInPartitions(const AggregatedDataVariants & variants) const;

    /// Merges partition `partition_index` of `num_partitions` — the keys whose two-level bucket `b` satisfies
    /// `b % num_partitions == partition_index` — out of every table of `non_empty_data` into a fresh table
    /// and converts it to one output chunk. The merge adopts the aggregate state pointers of
    /// first-seen keys and nulls the visited source cells, so distinct partitions may run concurrently
    /// over the same source tables and the tables' destruction afterwards cannot double-destroy.
    /// The NULL key of the single-key nullable methods belongs to partition 0.
    /// `max_source_table_size` (used to pre-size the destination table) must be measured by the
    /// caller before any partition starts: once the workers run, the source tables are mutated
    /// concurrently and may not be read outside the caller-owned partition.
    AggregatedChunk mergeSingleLevelPartitionAndConvertToChunk(
        ManyAggregatedDataVariants & non_empty_data,
        bool final,
        size_t partition_index,
        size_t num_partitions,
        size_t max_source_table_size,
        std::atomic<bool> & is_cancelled,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater) const;

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

    /// Flushes the variants like `writeToTemporaryFile` and consumes them: the table comes back
    /// invalidated and stripped of its arenas instead of re-armed for further aggregation, for
    /// callers that destroy it next.
    void consumeToTemporaryFile(AggregatedDataVariants & data_variants) const;

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
    friend class ConvertingAggregatedToChunksByPartitionMergingSource;
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

    /// The same, choosing the creation path the way the consume loop does: the compiled
    /// functions' states in one JIT call plus the rest generically when the query's functions
    /// are compiled, everything generically otherwise.
    void createAggregateStates(AggregateDataPtr & aggregate_data, bool use_compiled_functions) const;

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
        const ColumnRawPtrs & key_columns,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        bool all_keys_are_const,
        AggregateDataPtr overflow_row) const;

    /// The learning consume path of the adaptive aggregation, taken for a block that can push
    /// the table past the freeze threshold: aggregates like `executeImpl`, but in slices, and
    /// stops at the first row where the table stands at or past the threshold (the slicing
    /// gets it there with at most a slice of overshoot). Returns that boundary, or `row_end`
    /// when the block finished with the table still below; the transition and the routing of
    /// the rest of the block belong to the caller.
    size_t executeImplUntilAdaptiveFreeze(
        AggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions) const;

    /// Specialization for a particular value no_more_keys.
    template <bool prefetch, bool top_k = false, typename Method, typename State>
    requires MapAggregationState<State>
    void executeImplBatch(
        Method & method,
        State & state,
        const ColumnRawPtrs & key_columns,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        bool all_keys_are_const,
        bool use_compiled_functions,
        AggregateDataPtr overflow_row) const;

    struct DestroyedState
    {
        AggregateDataPtr slot;
        size_t row;
    };

    template <typename Method>
    void trimHeapAndPruneHashTable(Method & method, std::vector<DestroyedState> * destroyed_states, size_t current_row) const;

    /// A set method has no aggregate states: the batch only registers the keys.
    template <bool prefetch, bool top_k = false, typename Method, typename State>
    requires SetAggregationState<State>
    void executeImplBatch(
        Method & method,
        State & state,
        const ColumnRawPtrs & key_columns,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        bool all_keys_are_const,
        bool use_compiled_functions,
        AggregateDataPtr overflow_row) const;

    /// Registers keys without building aggregate states; shared by the set methods and by the
    /// no-aggregates fast path of the map methods.
    template <bool prefetch, bool top_k, typename Method, typename State>
    void executeImplBatchNoAggregates(
        Method & method,
        State & state,
        const ColumnRawPtrs & key_columns,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        bool all_keys_are_const) const;

    void initAdaptiveSession(AggregatedDataVariants & local_result, AdaptiveAggregationSession & shared) const;

    /// The freeze transition: initializes the session once, flips the producer's phase, and
    /// records the event. Owned here so the mid-block crossing and the between-blocks check
    /// perform the identical transition.
    void freezeAdaptive(AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive) const;

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
    requires MapAggregationMethod<LocalMethod>
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

    /// The set counterpart: with no aggregate functions there are no places to record and no states to
    /// advance, so a hit is just the probe and a miss stages the key alone.
    template <typename LocalMethod, typename SharedMethod>
    requires SetAggregationMethod<LocalMethod>
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
        bool counts_only,
        std::optional<UInt32> key_row_override = std::nullopt) const;

    /// Fills a value-staged block with the current misses grouped by bucket (and by a few hash
    /// bits within it, so a duplicate can only be one of its group's survivors) and merged:
    /// duplicate keys within the block collapse into one record with a summed run length, so a
    /// repeat-heavy staged stream copies each key's bytes once and the drain emplaces it once.
    template <typename SharedKey, typename State>
    void buildDeduplicatedCountChunk(
        StagedChunk & block,
        AdaptiveAggregationProducer & adaptive,
        State & local_find_state,
        Arena & scratch_pool,
        std::optional<UInt32> key_row_override) const;

    /// The aggregate-payload counterpart of `buildDeduplicatedCountChunk`: counting-sorts the
    /// staged misses into bucket-grouped order, stages their key bytes, and gathers the
    /// aggregate-argument columns into the same order (see `StagedChunk::AggregatePayload`).
    template <typename SharedKey, typename State>
    void buildBucketGroupedAggregateChunk(
        StagedChunk & block,
        const Columns & columns,
        AdaptiveAggregationProducer & adaptive,
        State & local_find_state,
        Arena & scratch_pool,
        std::optional<UInt32> key_row_override) const;

    /// Enqueues one batch for the merge-time drain: a batch of at least half the seal target
    /// goes straight to the backlogs, a small one is buffered, and the buffer is sealed into
    /// one chunk once enough bytes accumulate.
    void stageChunk(
        AdaptiveAggregationProducer & adaptive,
        MutableStagedChunkPtr block,
        size_t estimated_payload_bytes) const;

    /// Merges the buffered batches into one bucket-grouped chunk of the same shape (bucket b's
    /// records are the concatenation of the batches' b-slices) and enqueues it.
    void sealPendingChunks(AdaptiveAggregationProducer & adaptive) const;

    /// The value-staged variant of the seal merge: keys repeating across the batches collapse
    /// into one record with a summed run length while the records are copied into the chunk.
    void sealValueStagedChunkDeduplicated(
        const std::vector<MutableStagedChunkPtr> & minis,
        StagedChunk & chunk) const;

    /// The single publication point: finishes the chunk (builds its preparation in place,
    /// checks the structural invariants in debug builds) and hands it over as immutable to
    /// the session's backlog.
    void publishStagedChunk(AdaptiveAggregationSession & shared, MutableStagedChunkPtr block) const;

    /// Builds the staged chunk's shared preparation: the aggregate-function instructions over
    /// its argument columns, in the chunk's own stable storage.
    void prepareStagedChunk(StagedChunk & block) const;

    /// Drains one bucket's backlog into `method.data.impls[bucket_index]`. `key_storage`
    /// selects the ownership: merge-time drains emplace keys pointing into the retained
    /// chunks, while pressure-time drains persist them into the bucket's arena so the chunks
    /// can be freed (the whole point of draining early).
    template <AdaptiveKeyStorage key_storage, typename Method>
    size_t drainAdaptiveBucketBacklog(
        Method & method,
        Arena * arena,
        const std::vector<StagedChunkPtr> & backlog,
        size_t bucket_index,
        size_t total_records,
        PaddedPODArray<AggregateDataPtr> & places,
        std::atomic<bool> & is_cancelled) const;

    /// Applies one staged chunk's slice [slice_begin, slice_end) to the bucket's table.
    template <AdaptiveKeyStorage key_storage, typename Method>
    requires MapAggregationMethod<Method>
    void drainAdaptiveBucketImpl(
        Method & method,
        Arena * bucket_arena,
        const StagedChunk & block,
        size_t slice_begin,
        size_t slice_end,
        PaddedPODArray<AggregateDataPtr> & places,
        size_t bucket_index) const;

    /// The set counterpart: a staged key is emplaced and that is all - there is no state to create for
    /// a new key and nothing to advance for one already there.
    template <AdaptiveKeyStorage key_storage, typename Method>
    requires SetAggregationMethod<Method>
    void drainAdaptiveBucketImpl(
        Method & method,
        Arena * bucket_arena,
        const StagedChunk & block,
        size_t slice_begin,
        size_t slice_end,
        PaddedPODArray<AggregateDataPtr> & places,
        size_t bucket_index) const;

    void executeAggregateInstructions(
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        const AggregateFunctionInstruction * aggregate_instructions,
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

    void flushToTemporaryFile(AggregatedDataVariants & data_variants, size_t max_temp_file_size, bool reinitialize) const;

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
    requires MapAggregationMethod<Method>
    void mergeDataImpl(
        Table & table_dst, Table & table_src, Arena * arena, bool use_compiled_functions, bool prefetch,
        std::atomic<bool> & is_cancelled, const ParallelMergeWorker * parallel_worker = nullptr)
        const;

    /// Merge data from hash table `src` into `dst`, but only for keys that already exist in dst. In other cases, merge the data into `overflows`.
    template <typename Method, typename Table>
    requires MapAggregationMethod<Method>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena) const;

    /// A set method has no aggregate states, so there is nothing to merge or overflow.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena) const;

    /// A set method has no aggregate states: the merge is a plain key union.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
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
    requires MapAggregationMethod<Method>
    void mergeDataOnlyExistingKeysImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena) const;

    /// A set method has no aggregate states, so there is nothing to merge.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
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
    requires MapAggregationMethod<Method>
    Chunks
    convertToBlockImpl(Method & method, Table & data, Arena * arena, Arenas & aggregates_pools, bool final, size_t rows, bool return_single_block) const;

    /// A set method skips the inline-count and compiled-function paths; it only emits keys.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
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

    /// A set method has no aggregate states, so emitting its keys covers both the final and the non-final
    /// conversion; the map methods need the two below.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
    Chunks convertToBlockImplKeysOnly(
        Method & method, Table & data, Arenas & aggregates_pools, bool final, bool return_single_block) const;

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

    /// `topk_full_key_bytes`, when non-null and the bucket goes through the Top-K conversion,
    /// receives the byte size all of the bucket's keys would occupy materialized: the runtime
    /// dataflow statistics must describe the untruncated aggregation output (it prices the
    /// shipping term of the parallel-replicas plan, where the partial aggregation materializes
    /// every group), so the chunk of a truncated conversion cannot be measured as is.
    /// `full_group_count`, when non-null, receives the bucket table's group count: the group-by
    /// limit must be enforced against the true cardinality, which the chunk's row count
    /// understates when the Top-K conversion truncates it.
    template <typename Method>
    AggregatedChunk convertOneBucketToChunk(
        AggregatedDataVariants & data_variants,
        Method & method,
        Arena * arena,
        bool final,
        Int32 bucket,
        UInt64 * topk_full_key_bytes,
        size_t * full_group_count) const;

    AggregatedChunk convertOneBucketToChunk(AggregatedDataVariants & variants, Arena * arena, bool final, Int32 bucket) const;

    /// The bucket-local Top-K conversion (see `Params::bucket_top_k`): materializes only the
    /// bucket's n best cells by the plain count() state and destroys the rest, so the sorter
    /// upstream receives at most 256 * n candidate rows instead of every group.
    template <typename Method>
    requires MapAggregationMethod<Method>
    AggregatedChunk convertOneBucketToChunkTopK(
        Method & method, Arena * arena, Arenas & pools_for_output, Int32 bucket, UInt64 * full_key_bytes) const;

    /// `bucket_top_k` ranks groups by a lone `count()`, so it is never set for a set method, which has no
    /// aggregate functions at all. This overload exists only because the call site tests it at run time.
    template <typename Method>
    requires SetAggregationMethod<Method>
    AggregatedChunk convertOneBucketToChunkTopK(
        Method & method, Arena * arena, Arenas & pools_for_output, Int32 bucket, UInt64 * full_key_bytes) const;

    /// `full_group_count`, when non-null, receives the merged bucket's group count (see
    /// `convertOneBucketToChunk`).
    AggregatedChunk mergeAndConvertOneBucketToChunk(
        ManyAggregatedDataVariants & variants,
        Arena * arena,
        bool final,
        Int32 bucket,
        std::atomic<bool> & is_cancelled,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater,
        size_t * full_group_count) const;

    AggregatedChunk prepareChunkAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const;
    AggregatedChunks prepareChunksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final) const;

    template <bool return_single_block>
    std::conditional_t<return_single_block, AggregatedChunk, AggregatedChunks>
    prepareChunkAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const;

    template <typename Method>
    AggregatedChunks prepareChunksAndFillTwoLevelImpl(AggregatedDataVariants & data_variants, Method & method, bool final) const;

    /// The per-method body of `mergeSingleLevelPartitionAndConvertToChunk`'s merge. `TwoLevelMethod` is
    /// the method's two-level counterpart, whose bucket function defines the partition partition.
    template <typename Method, typename TwoLevelMethod>
    void mergeSingleLevelPartitionImpl(
        Method & dst_method,
        const std::vector<AggregatedDataVariants *> & sources,
        Arena * arena,
        size_t partition_index,
        size_t num_partitions,
        std::atomic<bool> & is_cancelled) const;

    template <typename State, typename Table>
    requires MapAggregationState<State>
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

    /// A set method has no aggregate states: merging a block back only re-registers its keys.
    template <typename State, typename Table>
    requires SetAggregationState<State>
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

    /// The instruction-building tail of `prepareAggregateInstructions`: the combinator
    /// unwrapping (-State, -Array) and the batch wiring for one aggregate whose argument
    /// pointers are already in place. Called directly for staged chunks, whose payload
    /// columns the seal already normalized to the drain's form.
    void buildAggregateFunctionInstruction(
        size_t i,
        bool has_sparse_arguments,
        AggregateColumns & aggregate_columns,
        AggregateFunctionInstructions & instructions,
        NestedColumnsHolder & nested_columns_holder) const;

    void addSingleKeyToAggregateColumns(
        AggregatedDataVariants & data_variants,
        MutableColumns & aggregate_columns) const;

    void addArenasToAggregateColumns(
        const AggregatedDataVariants & data_variants,
        MutableColumns & aggregate_columns) const;

    /// Appends the key row without creating an aggregate state. For callers that aggregate into
    /// the hash table and never read `data_variants.without_key`.
    void fillKeyColumnsWithSingleKey(
        Columns & key_columns, size_t key_row,
        MutableColumns & final_key_columns) const;

    /// Additionally creates a state in `data_variants.without_key`. The caller must transfer its
    /// ownership with `addSingleKeyToAggregateColumns`, or the state is leaked.
    void createStatesAndFillKeyColumnsWithSingleKey(
        AggregatedDataVariants & data_variants,
        Columns & key_columns, size_t key_row,
        MutableColumns & final_key_columns) const;

    static bool hasSparseArguments(const AggregateFunctionInstruction * aggregate_instructions);

    static void addBatch(
        size_t row_begin, size_t row_end,
        const AggregateFunctionInstruction * inst,
        AggregateDataPtr * places,
        Arena * arena);

    static void addBatchSinglePlace(
        size_t row_begin, size_t row_end,
        const AggregateFunctionInstruction * inst,
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
