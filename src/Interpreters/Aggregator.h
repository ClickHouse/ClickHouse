#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
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

#include <Interpreters/AdaptiveAggregation/AdaptiveAggregation.h>
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
struct MemoryTrackerSwitcher;
class NativeWriter;
struct OutputBlockColumns;
class PartitionedRecordBuffer;

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

/// What a conversion that materializes only some of the bucket's groups - the Top-K one or the HAVING
/// pre-filter - reports to the runtime dataflow statistics about the groups it did not materialize.
struct UntruncatedAggregationKeys
{
    /// What every group's key - kept or skipped - would occupy materialized.
    UInt64 bytes = 0;

    /// A bounded copy of those keys, in key order, or empty. The statistics divide the byte count by a
    /// compression ratio, which the emitted chunk can only misrepresent: it holds the kept groups, and
    /// a conversion that rejected every group leaves no row there at all - and without a ratio the byte
    /// count is dropped instead of estimated.
    Columns sample_columns;
};

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
        AggregateDescriptions aggregates;
        size_t aggregates_size = 0;

        ///
        /// The settings of approximate calculation of GROUP BY.
        ///
        /// Do we need to put into AggregatedDataVariants::without_key aggregates for keys that are not in max_rows_to_group_by.
        const bool overflow_row = false;
        size_t max_rows_to_group_by = 0;
        OverflowMode group_by_overflow_mode = OverflowMode::THROW;

        /// Two-level aggregation settings (used for a large number of keys).
        /// With how many keys or the size of the aggregation state in bytes,
        /// two-level aggregation begins to be used. Enough to reach of at least one of the thresholds.
        /// 0 - the corresponding threshold is not specified.
        size_t group_by_two_level_threshold = 0;
        size_t group_by_two_level_threshold_bytes = 0;

        /// Settings to flush temporary data to the filesystem (external aggregation).
        /// 0 - do not use external aggregation.
        size_t max_bytes_before_external_group_by = 0;
        size_t min_bytes_before_external_group_by = 0;
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
        bool adaptive_aggregator_disable_thaw = false;

        bool use_aggregation_memory_tracker = true;

        /// Bucket-local Top-K of the final conversion, set by the `aggregation_bucket_top_k`
        /// plan optimization (never by users) when the plan proves this aggregation feeds
        /// `ORDER BY` with `LIMIT n` by a count: the output of the aggregate
        /// `bucket_top_k_rank_index`, a lone `count`, `uniqExact` or `uniqExactIf`. Each two-level bucket
        /// materializes only its n best cells by that count. Exact, because a group outside its
        /// own bucket's best n has at least n groups ahead of it globally. Zero disables. Kept
        /// out of the constructor and off the wire deliberately: it is a property of the plan
        /// shape, and a deserialized plan re-runs without the optimization, which is the safe direction.
        size_t bucket_top_k = 0;
        bool bucket_top_k_ascending = false;
        size_t bucket_top_k_rank_index = 0;

        /// The largest `bucket_top_k` the plan optimization sets. The per-bucket selection scans every cell anyway; past
        /// this many candidate rows per bucket the saved materialization no longer pays for the selection scan.
        static constexpr size_t max_bucket_top_k = 65536;

        /// Whether the adaptive aggregation prunes its merge by the top-K of `bucket_top_k` (see `AdaptiveTopKPruning`):
        /// the count bins bound the ranks from above, which serves a descending order only, and a throw-mode group limit
        /// needs every group counted, which a skipped unit is not.
        bool adaptiveTopKPrunes() const { return bucket_top_k && !bucket_top_k_ascending && !max_rows_to_group_by; }

        /// Whether a frozen producer of the adaptive aggregation may thaw: not with `adaptive_aggregator_disable_thaw`,
        /// and not under the top-K pruning. There the frozen tables pay even for a repetitive stream: the merge skips
        /// the units whose groups cannot reach the top, which a thawed table, a source of every unit, would no longer
        /// allow.
        bool adaptiveMayThaw() const { return !adaptive_aggregator_disable_thaw && !adaptiveTopKPrunes(); }

        /// Top-K threshold merge (Fagin's Threshold Algorithm), set by the same plan optimization
        /// as `bucket_top_k` (never by users) when the plan proves this aggregation feeds
        /// `ORDER BY <the output of a single aggregate with a declared MergedValueBound> LIMIT n`.
        /// Instead of merging every group of a two-level bucket across the per-thread hash tables
        /// and materializing all of them, the merge walks the tables in the order of the partial
        /// values of that aggregate and stops as soon as no unseen group can rank among the
        /// bucket's n best, so only the candidate groups are merged and materialized. Exact for
        /// the same reason as `bucket_top_k`: a group outside its own bucket's best n has at
        /// least n groups ahead of it globally. Kept out of the constructor and off the wire
        /// deliberately, like `bucket_top_k`.
        struct ThresholdTopKParams
        {
            size_t k = 0;
            bool ascending = false;
            /// Index in `aggregates` of the function the query orders by.
            size_t aggregate_index = 0;
            MergedValueBound bound = MergedValueBound::Unknown;
        };
        std::optional<ThresholdTopKParams> threshold_top_k;

        /// A bound on the aggregate at `having_prefilter_count_index`, a no-argument `count()`, whose rejected
        /// groups may be skipped. The filter above stays authoritative, so skipping fewer is still correct.
        enum class HavingPrefilterOp : UInt8
        {
            Disabled,
            Greater,
            GreaterOrEqual,
            Less,
            LessOrEqual,
            Equal,
        };
        HavingPrefilterOp having_prefilter_op = HavingPrefilterOp::Disabled;
        UInt64 having_prefilter_threshold = 0;
        size_t having_prefilter_count_index = 0;

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
            bool shared_boundary = true;            /// let the per-thread sets skip against the tightest boundary published by any thread

            /// Set by the plan optimization when the first ranked key is a column read straight from a `MergeTree`
            /// table or a Parquet file: the heaps publish their boundary into it and the reading step filters rows and skips granules
            /// by the published value (see `enable_group_by_top_k_dynamic_filtering`). Never serialized: it is a
            /// link between two steps of the same local plan.
            TopKThresholdTrackerPtr threshold_tracker;
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

        /// With `group_by_overflow_mode = ANY`: once any aggregation stream exceeds
        /// `max_rows_to_group_by`, freeze a single shared set of exactly `max_rows_to_group_by`
        /// kept keys and restrict every parallel stream to that set, so that the aggregate values
        /// of the returned keys stay exact. Without it the cutoff is per-stream: a key kept by one
        /// stream and rejected by another loses the other stream's rows and comes out of the merge
        /// with an undercounted value. Set by the trivial `GROUP BY ... LIMIT` optimization for
        /// local single-stage aggregation (see `addAggregationStep` in the planner); requires
        /// `ANY` overflow mode without an overflow row.
        /// For the fixed hash map methods (8/16-bit keys) the cutoff stays inert and the
        /// aggregation is exact and complete — see `Aggregator::shared_kept_keys_cutoff_inert`.
        /// See `ManyAggregatedData::SharedKeptKeys` for the protocol.
        bool shared_kept_keys_for_overflow_any = false;

        /// Runtime arbitration between the kept-keys cutoff and external aggregation, shared by
        /// every stream (and every `Aggregator`) of one aggregation step. Spilled buckets would
        /// bypass the restriction to the kept keys, so before the kept keys are frozen the two
        /// exclude each other, decided by whichever fires first:
        /// - a stream that needs to spill before any freeze permanently abandons the cutoff
        ///   (`checkLimits` stops capping the tables, no rows have been dropped anywhere, and the
        ///   aggregation completes exactly, spilling as it would without the optimization);
        /// - a freeze locks the cutoff in, and from then on only a table already rebuilt to the
        ///   kept keys may spill.
        /// After a freeze, capping the number of keys is not by itself a memory bound: the kept
        /// keys are bounded, but their aggregate states are not (`uniqExact`, `groupArray`,
        /// `topK`, the exact quantiles), so external aggregation has to stay available. A table
        /// rebuilt to the kept keys holds nothing but kept keys, so flushing it to disk cannot
        /// leak a dropped key into the result; the flush empties the table, which under
        /// `no_more_keys` would drop the remaining rows of the kept keys, so the `Aggregator`
        /// re-seeds it from `AggregatedDataVariants::kept_keys_seed` right after the flush and
        /// keeps aggregating exactly.
        /// A table that has not been rebuilt yet may not spill — it still holds arbitrary keys.
        /// Created by `AggregatingStep` when the cutoff is armed; null otherwise.
        struct SharedKeptKeysControl
        {
            enum class State : UInt8
            {
                Armed,
                Frozen,
                Abandoned,
            };

            std::atomic<State> state{State::Armed};

            /// The freezing stream claims the cutoff before dismantling its hash table.
            /// Fails when a spill has abandoned the cutoff or another stream froze it first.
            bool tryFreeze()
            {
                State expected = State::Armed;
                return state.compare_exchange_strong(expected, State::Frozen);
            }

            /// A stream that needs to spill abandons the cutoff. Fails (the spill must be
            /// skipped) when the kept keys are already frozen.
            bool tryAbandon()
            {
                State expected = State::Armed;
                return state.compare_exchange_strong(expected, State::Abandoned) || expected == State::Abandoned;
            }

            bool isAbandoned() const
            {
                return state.load() == State::Abandoned;
            }
        };

        std::shared_ptr<SharedKeptKeysControl> shared_kept_keys_control;

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
            size_t min_bytes_before_external_group_by_,
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
            UInt64 adaptive_aggregator_freeze_threshold_bytes_,
            bool adaptive_aggregator_disable_thaw_,
            bool use_aggregation_memory_tracker_);

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

        Params cloneWithKeysAndAggregates(const Names & keys_, const AggregateDescriptions & aggregates_, bool only_merge_ = false) const
        {
            Params new_params = cloneWithKeys(keys_, only_merge_);
            new_params.aggregates = aggregates_;
            new_params.aggregates_size = aggregates_.size();
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

    /// Creates the shared state of an adaptive aggregation over `producers` streams: their partition layout, the scope
    /// of the spill streams when the aggregation may spill, and the top-K pruning when the aggregation feeds one.
    AdaptiveAggregationSessionPtr createAdaptiveSession(size_t producers) const;

    /// Ends a producer's production when its input ends, whatever its phase. Hands its partitions to the session with
    /// its count bins when the aggregation prunes by the top-K; the bins take the rows of `local_variants`, the
    /// producer's own table, as well. A producer that never froze has no bins and hands `local_variants` over for the
    /// merge to count instead. Counts the producer toward the verdict of the run, and converts the table of a producer
    /// that froze to two-level for the merge. Every producer with the adaptive context calls it before the finish
    /// barrier, so the session holds every producer's contribution by the time the last finisher assembles the merge.
    void finishAdaptiveProducer(AggregatedDataVariants & local_variants, AdaptiveAggregationProducer & adaptive) const;

    /// Serves a spill request of the memory scheduler for an adaptive producer: writes its staged records to the
    /// session's spill streams and, if that releases less than `at_least_bytes`, its table to disk. A table on the
    /// adaptive path is written as over the external-aggregation threshold, and its producer learns again; a table on
    /// the baseline path is spilled as with the feature off. The staged records go first: the merge reads them back
    /// unit by unit, while a written table takes the whole merge external. Unlike the writes over the threshold, these
    /// skip the floor of `adaptive_spill_min_bytes`: the scheduler asks for memory now, in amounts of at least
    /// `min_bytes_to_spill`, while the floor keeps a query that stays over the threshold from writing on every block.
    /// Returns the released bytes.
    size_t spillAdaptiveProducer(
        AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive, bool no_more_keys, size_t at_least_bytes) const;

    /// Serves a spill request of the memory scheduler for the staged records that finished producers handed over to the
    /// session (see `AdaptiveAggregationSession::producer_buffers`): writes them all to the session's spill streams and
    /// frees them, while their spills have not ended at the finish barrier. Returns the released bytes.
    size_t spillHandedOverStagedRecords(AdaptiveAggregationSession & shared) const;

    /// Decides once, after the finish barrier of an engaged aggregation and before its merge starts, whether the merge
    /// prunes by the top-K, and orders the buckets for it by their bounds. The bounds need every source's rows, in the
    /// count bins or in a table that the merge counts. A producer without the adaptive context (another aggregator of a
    /// mixed projection pipeline) handed over neither, and an external merge reads back rows that nothing counted. A
    /// merge that collects the runtime dataflow statistics needs every group converted. In all these cases the pruning
    /// is dropped.
    void prepareAdaptiveTopKPruning(
        AdaptiveAggregationSession & shared, size_t producers, bool external_merge, bool collects_statistics) const;

    /// Merges bucket `bucket` of an adaptive aggregation and converts it, one merge unit at a time, as a step of `task`.
    /// A unit is a run of the bucket's partitions: its records from every producer and the cells of the sources' bucket
    /// tables that fall into it are merged into the destination's bucket table, grown for them, which is converted into
    /// one chunk, and the unit's staged records are freed. `data[0]` is the destination of
    /// `createAdaptiveMergeDestination`: in front of the adaptive sources (see `prepareVariantsToMerge`), or with no
    /// sources behind it for an external merge. The table is kept, emptied, from one unit to the next, and taken over
    /// from the slot of the bucket the task merged before, so a task allocates and faults in one table instead of one
    /// per unit. Each unit creates its states in a fresh arena of the bucket's arena slot, so the states of a converted
    /// unit free with its arena, at once or with the chunk that captured it, and once the bucket's last unit converted,
    /// the slot retires. A unit reads the spill streams of its partitions back a block at a time and drains each block
    /// before the next, so the merge holds the unit's groups instead of all its spilled records; a count-first unit,
    /// which passes over its records twice, reads the spill streams back twice. `full_group_count` receives the
    /// bucket's group count over all its units, taken before any top-K truncation.
    AggregatedChunks mergeAndConvertAdaptiveBucket(
        ManyAggregatedDataVariants & data,
        AdaptiveAggregationSession & session,
        AdaptiveMergeTask & task,
        bool final,
        Int32 bucket,
        std::atomic<bool> & is_cancelled,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater,
        size_t * full_group_count) const;

    /// The destination of the adaptive merge: an empty two-level variants of the aggregation's method with an arena slot
    /// per bucket, which holds the arena in which the merge creates the states of the bucket's unit. For a merge that goes
    /// external because producers wrote tables to disk (on the baseline path, or frozen ones), the staged records
    /// join the external merge as chunks of aggregate states, the form of a spilled part, which
    /// `mergeAndConvertAdaptiveBucket` makes with no sources behind the destination.
    AggregatedDataVariantsPtr createAdaptiveMergeDestination() const;

    /// The header of the blocks of aggregate states that the external aggregation spills and merges.
    Block getSpilledStatesHeader() const;

    /// Holds a throw-mode `max_rows_to_group_by` against the groups of the buckets merged so far: adds
    /// `bucket_groups`, the groups of the bucket just merged, to `merged_groups`, the running total of the
    /// threads that merge the buckets, and checks the total. The buckets partition the key space, so the
    /// total never exceeds the final group count: once it crosses the limit, so does the final count. The
    /// producers' checks miss what this catches: the baseline producers check their tables one by one, so
    /// they cannot see the merged cardinality, and the adaptive producers cannot see the staged keys at all.
    /// The dropping modes keep the merge untouched: their contract is decided at the producers, and stopping
    /// the merge here would drop already-aggregated groups.
    void checkMergedGroupLimit(std::atomic<size_t> & merged_groups, size_t bucket_groups) const;

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
      * A non-zero `max_rows_per_block` caps the size of the emitted single-level chunks below `max_block_size`.
      */
    AggregatedChunks convertToChunks(AggregatedDataVariants & data_variants, bool final, size_t max_rows_per_block = 0) const;

    /// A single-level result smaller than `max_block_size` is converted to one chunk, and a `Resize`
    /// hands out whole chunks, so everything downstream of it runs in one thread. Returns a chunk size
    /// that splits `rows` into about one chunk per output stream, never below 512 rows per chunk, or 0
    /// to leave the result as is.
    static size_t singleLevelChunkRowsForFanOut(size_t rows, size_t output_streams);

    /// Records the adaptive verdict of the run in the hash-table statistics when the run measured one (see
    /// `adaptiveStagingVerdict`). The in-memory merge records it inside `prepareVariantsToMerge`; the external merge
    /// never reaches that, so its finish path calls this instead.
    void recordAdaptiveStagingVerdict(const AdaptiveAggregationSession & shared) const;

    /// `adaptive_session` (or nullptr when the adaptive aggregation is off) puts the destination of the adaptive merge
    /// in front of the tables when the session is engaged. It also gives the adaptive verdict of the run to the
    /// hash-table statistics.
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

    /// Flushes the table on request of the memory scheduler; returns the released bytes.
    /// Requires a two-level or convertible table and temporary data storage.
    size_t spill(AggregatedDataVariants & data_variants, bool no_more_keys) const;

    bool hasTemporaryData() const;

    /// Peak memory of the aggregation state across all threads. Unavailable when the states
    /// arrive pre-allocated (merge-only aggregation) and cannot be tracked.
    std::optional<UInt64> getPeakMemoryUsage() const;

    std::list<TemporaryBlockStreamHolder> detachTemporaryData();

    /// Part of automatic parallel replicas implementation.
    size_t estimateSizeOfCompressedState(AggregatedDataVariants & result, ssize_t bucket) const;

    const ColumnNumbers & getKeysPositions() const { return keys_positions; }
    const DataTypes & getKeyTypes() const { return key_types; }

    const TemporaryDataOnDiskScope * getSpillScope() const { return tmp_data.get(); }


private:

    friend struct AggregatedDataVariants;
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

    /// The record layout of the staged aggregate arguments; set when the adaptive aggregation is engaged and the
    /// query has aggregate functions.
    std::unique_ptr<const AdaptiveArgumentLayout> adaptive_argument_layout;
    /// Per-query aggregate metadata used to estimate the cost of retaining states and merging them.
    size_t adaptive_state_bytes_per_distinct_input = 0;
    /// The only aggregate whose state grows per distinct input; its arguments are sampled when
    /// `adaptive_state_bytes_per_distinct_input` is non-zero.
    size_t adaptive_distinct_input_aggregate = 0;
    std::vector<size_t> adaptive_parallel_merge_indices;

    /// The tightest top-K skip boundary any aggregation thread has published; shared by the
    /// per-thread heaps of this aggregation (see `SharedTopKBoundary`). Mutable because the
    /// execution paths that publish and read it are `const`.
    mutable SharedTopKBoundary top_k_shared_boundary;

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

    /// `shared_kept_keys_for_overflow_any` is requested, but `method_chosen` uses a fixed hash
    /// map (8/16-bit keys), where the cutoff is both useless and broken: the tables are bounded
    /// by the key space (at most 65536 cells), so capping them buys nothing, and the implicit-zero
    /// maps cannot represent a kept key with a zero inline `count()` state (such a cell reads as
    /// absent, so the seeded keys would be lost and their rows dropped). `checkLimits` ignores
    /// `max_rows_to_group_by` in that case and the aggregation stays exact and complete.
    bool shared_kept_keys_cutoff_inert = false;

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

    /// Whether any of the aggregate functions supports parallel merging of large states
    /// (see IAggregateFunction::isAbleToParallelizeMerge), e.g. uniqExact.
    bool has_function_with_parallelizable_merge = false;

    /// How many RAM were used to process the query before processing the first block. Use for merge_only mode.
    Int64 memory_usage_before_aggregation = 0;
    /// Track memory held by the aggreagation state during execution.
    std::unique_ptr<MemoryTracker> memory_tracker;

    /// Redirects the thread's accounting to the per-table tracker (created on demand under the
    /// aggregator tracker). Returns the aggregator tracker, nullptr when there is none to switch to.
    MemoryTracker * switchToOwnTracker(AggregatedDataVariants & result, std::optional<MemoryTrackerSwitcher> & switcher) const;

    /// The aggregator tracker when the current thread reports to the query it accounts for, nullptr otherwise.
    MemoryTracker * aggregatorTracker() const;

    /// Indicates whether the aggregation is a simple `count()` / `count(*)` / `count(non-nullable_column)`
    ///
    /// If true, we can apply an important performance optimization:
    /// - The aggregation logic can be inlined, meaning each row is aggregated immediately during hash table probing.
    /// - There's no need to allocate and maintain full aggregation state.
    bool is_simple_count = false;

    /// How a group's rank by the aggregate of `Params::bucket_top_k` is read: the inline count of an aggregation by a
    /// lone `count`, the state of a `count` among other aggregates, which is the count itself, or the result of
    /// `uniqExact` or `uniqExactIf`, which `finalizeBucketTopKRank` reads from its state.
    enum class BucketTopKRank : UInt8
    {
        InlineCount,
        CountState,
        FinalizedResult,
    };

    /// Set when `Params::bucket_top_k` is.
    BucketTopKRank bucket_top_k_rank = BucketTopKRank::InlineCount;

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

    /// `for_sub_range`: the rows are one run of a block aggregated run by run (`executeOnBlockSmall`), so the
    /// hashing state is a `ColumnsHashing::SubRangeState` built over those rows only.
    template <bool for_sub_range = false>
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
    template <bool for_sub_range, typename Method>
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

    /// Advances an adaptive producer's phase between blocks, in place of the baseline's conversion: writes its staged
    /// records to disk when the query is `over_external_threshold`, thaws a frozen table whose staged stream repeats,
    /// and freezes a learning table at its bounds. A learning table counts the block's `rows` toward the end of its
    /// slicing (see `adaptiveLearningSlices`). Returns false once the producer is on the baseline path, whose checks
    /// then apply.
    bool advanceAdaptiveProducer(
        AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive, size_t rows, bool over_external_threshold) const;

    /// Whether a learning table reached a bound of the freeze: its key count or its own tracked memory, so the caller
    /// reports the thread's unreported allocations to the table's tracker first (see `executeOnBlock`).
    bool adaptiveFreezeBoundReached(const AggregatedDataVariants & result) const;

    /// Whether a learning table still slices its blocks to freeze exactly at the key-count threshold: until it has
    /// taken more than `adaptive_learning_slicing_row_multiple` times the threshold in rows.
    bool adaptiveLearningSlices(const AdaptiveAggregationProducer & adaptive) const;

    /// Whether a producer over the external-aggregation threshold holds enough staged records to write them to disk.
    bool adaptiveStagingWorthSpilling(const AdaptiveAggregationProducer & adaptive) const;

    /// Whether writing a learning or frozen table to disk over the external-aggregation threshold frees enough memory:
    /// a learning table holds the floor of `adaptive_spill_min_bytes` in its tracked memory, a frozen one's states grew
    /// past the floor since the freeze, in its tracked memory or in its arenas. For a learning table the caller reports
    /// the thread's unreported allocations to the table's tracker first (see `executeOnBlock`).
    bool adaptiveTableWorthSpilling(const AggregatedDataVariants & result, const AdaptiveAggregationProducer & adaptive) const;

    /// The freeze transition: creates the producer's partitions and count bins at its first freeze, flips its phase,
    /// and records the event. Owned here so the mid-block crossing and the between-blocks check perform the identical
    /// transition. The frozen phase takes the table's arena bytes and tracked memory as the bases of its spill guard
    /// (see `adaptiveTableWorthSpilling`), so the caller reports the thread's unreported allocations to the table's
    /// tracker first (see `executeOnBlock`).
    void freezeAdaptive(AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive) const;

    /// The thaw transition: the producer returns to the baseline path for good, keeping the records it staged.
    void thawAdaptive(const AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive) const;

    /// Merges the source states of a merge unit into their destinations, the state of `*source_slots[i]` into
    /// `places[i]`, then destroys the sources and clears their slots. The source tables keep ownership until all
    /// aggregates merged successfully. `task` holds the working memory of the merge. A group's states use the pool
    /// when their combined work amortizes its setup or would hold up the remaining merge; smaller groups use the
    /// parallelism across buckets.
    void mergeAdaptiveSourceStates(
        PaddedPODArray<AggregateDataPtr> & places,
        const PaddedPODArray<AggregateDataPtr *> & source_slots,
        AdaptiveMergeTask & task,
        const AdaptiveAggregationSession & session,
        Arena * arena,
        std::atomic<bool> & is_cancelled) const;

    /// The adaptive verdict of a finished run: whether its staged streams were repeat-dominated, so that later runs of
    /// the query should not engage the adaptive aggregation, or nothing when the run measured nothing.
    std::optional<bool> adaptiveStagingVerdict(const AdaptiveAggregationSession & shared) const;

    /// Compares a frozen producer's repeated staging cost with the states its ordinary table would retain.
    bool adaptiveStagingWastes(
        const AdaptiveAggregationProducer & adaptive, size_t state_bytes_per_key, size_t state_cost_multiplier) const;

    /// The thaw verdict of a frozen producer, checked between blocks: its own staged stream repeats its misses so much
    /// that its table would do better absorbing them in place (see the tuning constants in `AdaptiveAggregationImpl.h`).
    bool adaptiveStagingRepeats(const AdaptiveAggregationProducer & adaptive) const;

    /// The frozen consume path: rows whose key the local table holds are aggregated in place,
    /// the other rows are appended to the producer's partitions as delayed records for the merge.
    /// The growth of the local states goes to `local_memory_tracker` when it is given, the staging to
    /// the caller's tracker.
    void executeFrozen(
        const Columns & columns,
        size_t row_begin,
        size_t row_end,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        AdaptiveAggregationProducer & adaptive,
        bool all_keys_are_const,
        MemoryTracker * local_memory_tracker) const;

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
        bool all_keys_are_const,
        MemoryTracker * local_memory_tracker) const;

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
        bool all_keys_are_const,
        MemoryTracker * local_memory_tracker) const;

    /// Collects the misses of a block of an aggregation by a lone `count` into the producer's batch: a row whose key the
    /// local table holds adds one to the key's inline count, and the other rows become count records, each with the
    /// multiplicity of the rows it combines. A run of one key goes into one record, and while a trial shows that it
    /// pays, so do the later rows of the key found through a per-partition cache. A batch whose kept key bytes reach
    /// their bound is staged by `stage_misses` in the middle of the block (see `adaptive_miss_batch_max_key_bytes`); the
    /// rest of the misses and the probe results are left to the caller. Returns the number of local hits.
    template <typename RecordKey, typename LocalMethod, typename State, typename StageMisses>
    size_t collectCountMisses(
        LocalMethod & local_method,
        State & find_state,
        size_t row_begin,
        size_t row_end,
        Arena & scratch_pool,
        AdaptiveAggregationProducer & adaptive,
        bool bypass_local_probe,
        const StageMisses & stage_misses) const;

    /// Appends the producer's batch of misses to its partitions in the format `visitStagedRecordFormat` selects. Each
    /// record carries a key and either a multiplicity (an aggregation by a lone `count`), no payload (no aggregates), or
    /// the row's aggregate arguments laid out by `adaptive_argument_layout`, read from the argument columns of the
    /// block's `aggregate_instructions`. Variable-size records also carry a hash and lengths; fixed-size records
    /// recover the hash from the key when drained. Returns what it staged for the accounting of the batch (see
    /// `accountStagedBatch`).
    template <typename RecordKey, typename State>
    AdaptiveStagedBytes appendDelayedRecords(
        const AggregateFunctionInstruction * aggregate_instructions,
        AdaptiveAggregationProducer & adaptive,
        State & local_find_state,
        Arena & scratch_pool) const;

    /// Accounts the producer's batch of misses, which `appendDelayedRecords` staged with `staged`, then clears the
    /// batch: adds the batch's rows to the count bins, its records to the merge-work estimate and to the profile events,
    /// and its footprint and sampled hashes to the thaw evidence (see `adaptiveStagingRepeats`).
    void accountStagedBatch(const Columns & columns, AdaptiveAggregationProducer & adaptive, const AdaptiveStagedBytes & staged) const;

    template <typename Method>
    AggregatedChunks mergeAndConvertAdaptiveBucketImpl(
        AggregatedDataVariants & dest,
        Method & dest_method,
        ManyAggregatedDataVariants & data,
        AdaptiveAggregationSession & session,
        AdaptiveMergeTask & task,
        bool final,
        Int32 bucket,
        std::atomic<bool> & is_cancelled,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater,
        size_t * full_group_count) const;

    /// Drains one partition's records into `table`, only those whose routing hash `filter` takes: every record, those
    /// whose count bin the top-K pruning keeps alive (see `AdaptiveTopKPruning`), or those of the best groups of a
    /// count-first unit. New groups get their states in `arena`, the arena of the unit. String-like keys are emplaced
    /// pointing into the records, which are freed only after the table is converted or written, unless `copy_keys`:
    /// records read back from a spill go before, so their new keys are copied into `arena` too. Returns how many
    /// records it skipped.
    template <typename Method, bool copy_keys, typename Table, typename Filter>
    size_t drainAdaptivePartition(
        Table & table,
        Arena * arena,
        const AdaptiveRecordRanges & ranges,
        const Filter & filter,
        PaddedPODArray<AggregateDataPtr> & places,
        RowStorePointers & records) const;

    /// Counts the groups of one partition's argument records into `counting_table`, only those whose routing hash
    /// `filter` takes: a record adds one row to the count its group keeps in the mapped value, as a lone `count()`
    /// keeps it, and builds no state. `update_candidates(count, key, hash, copy_key)` then takes the grown count by
    /// reference, so a tracker of the best groups can keep its metadata in the count's high bits. String-like keys
    /// point into the records, unless `copy_keys`: records read back from a spill go before the counting ends, so the
    /// counting table copies its new keys into `arena`, and the tracker keeps copies of its own, which `copy_key`
    /// asks for. Returns how many records it skipped.
    template <typename Method, bool copy_keys, typename Table, typename Filter, typename UpdateCandidates>
    requires MapAggregationMethod<Method>
    size_t countPartitionGroups(
        Table & counting_table,
        Arena * arena,
        const AdaptiveRecordRanges & ranges,
        const Filter & filter,
        const UpdateCandidates & update_candidates) const;

    /// Destroys the states of a source cell whose group cannot reach the top, and clears the cell's slot.
    void discardAdaptiveSourceState(AggregateDataPtr & place) const;

    /// Takes the source cells of a merge unit that `filter` takes into `table`, and discards the others with their
    /// states. A cell whose key is new in `table` hands its state over, and an aggregation by a lone `count` adds the
    /// count. Otherwise the cell's state is to merge into the table's: its place and its slot go to `task.places` and
    /// `task.source_slots` for `mergeAdaptiveSourceStates`.
    template <typename Method, typename Table, typename Cells, typename Filter>
    void adoptSourceCells(Table & table, Cells & cells, const Filter & filter, AdaptiveMergeTask & task) const;

    /// The first pass of a count-first unit: counts the unit's groups exactly in `counting_table`, from its source
    /// cells, the records in memory of every partition `for_each_in_memory_partition` hands out and the records of
    /// every spilled block `for_each_spilled_block` reads back, in the count bins `alive_bins` keeps (all bins when
    /// null), and puts the hashes of its best groups by the count into `task.best_hashes`. The counting table and
    /// `top_k` copy the string-like keys of spilled records they keep into `arena`. With a descending order `top_k`
    /// keeps the best groups up to date while they are counted, and `pruning`, if any, drops those below its
    /// threshold; an ascending order selects them by a pass over the counted groups. Adds the records the bins
    /// skipped to `pruned_records`. Returns the unit's group count.
    template <
        typename Method,
        typename Table,
        typename Cells,
        typename TopK,
        typename ForEachInMemoryPartition,
        typename ForEachSpilledBlock>
    size_t selectCountFirstBestGroups(
        Table & counting_table,
        const Cells & cells,
        const bool * alive_bins,
        TopK * top_k,
        const AdaptiveTopKPruning * pruning,
        const ForEachInMemoryPartition & for_each_in_memory_partition,
        const ForEachSpilledBlock & for_each_spilled_block,
        Arena * arena,
        AdaptiveMergeTask & task,
        size_t & pruned_records) const;

    /// Rebuilds the aggregate arguments of drained argument records of `format` (see `visitStagedRecordFormat`) into
    /// dense columns and applies them to the places of their groups (see `executeStagedArguments`).
    template <typename Record>
    void executeStagedArgumentRecords(
        const Record & format,
        Arena * arena,
        PaddedPODArray<AggregateDataPtr> & places,
        const RowStorePointers & records,
        bool use_compiled_functions) const;

    /// Applies the argument columns rebuilt from drained records to the places of their groups, as the batch executor
    /// applies a block.
    void executeStagedArguments(
        Arena * arena, PaddedPODArray<AggregateDataPtr> & places, const Columns & arguments, bool use_compiled_functions) const;

    /// Estimates the work of the selected aggregate states and, when requested, adds rank counts to the producer's
    /// top-K bins. Both statistics visit the same cells before the producer hands its table to the merge.
    size_t collectAdaptiveTableStatistics(AggregatedDataVariants & variants, AdaptiveTopKCountBins * bins) const;
    template <typename Method>
    size_t collectAdaptiveTableStatistics(Method & method, Arena * arena, AdaptiveTopKCountBins * bins) const;

    /// The rank of the group with the aggregate states at `mapped`, which the top-K pruning bounds: its count, or the
    /// distinct count of its `uniqExact` or `uniqExactIf` rank, finalized into `scratch`.
    UInt64 adaptiveRankCount(AggregateDataPtr & mapped, IColumn * scratch, Arena * arena) const;

    /// Adds the groups that the tables of the producers that never froze hold in bucket `bucket` (see
    /// `AdaptiveTopKPruning::isMergeCounted`) to the bucket's `bounds`. It stays out of line: inlined into the bucket
    /// merge, its loops change the register allocation of the merge's hot loops.
    template <typename Method>
    void addMergeCountedTablesToBounds(
        ManyAggregatedDataVariants & data, Int32 bucket, const AdaptiveTopKPruning & pruning, std::span<UInt64> bounds, Arena * arena) const;

    /// Writes the staged records of `partitions`, a producer's or a finished producer's handed over to `shared`, to the
    /// session's spill streams, a block per partition, and frees them; a producer then keeps appending into new chunks.
    /// Called over the external-aggregation threshold and on request of the memory scheduler (see
    /// `spillAdaptiveProducer` and `spillHandedOverStagedRecords`).
    void spillAdaptivePartitions(AdaptiveAggregationSession & shared, PartitionedRecordBuffer & partitions) const;

    /// Writes the producer's learning or frozen table to disk as a part of the ordinary external aggregation, then lets
    /// the producer learn its keys again in an empty single-level table. Called for a table over the
    /// external-aggregation threshold when the write frees enough memory (see `adaptiveTableWorthSpilling`), and on
    /// request of the memory scheduler (see `spillAdaptiveProducer`).
    void spillAdaptiveTable(AggregatedDataVariants & result, AdaptiveAggregationProducer & adaptive, size_t max_temp_file_size) const;

    void executeAggregateInstructions(
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        const AggregateFunctionInstruction * aggregate_instructions,
        AggregateDataPtr * places,
        size_t key_start,
        bool has_only_one_value_since_last_reset,
        bool all_keys_are_const,
        bool all_places_are_non_null,
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

    /// Pairs of states (with the function's state offset applied) whose merge `mergeDataImpl` deferred
    /// because they are too large to merge serially; one entry per aggregate function.
    /// A destination repeats once per source it has to absorb, so after grouping by destination the
    /// group is merged in one parallel multi-way pass (`mergeDeferredLargeStates`).
    struct DeferredMergePairs
    {
        PaddedPODArray<AggregateDataPtr> dst_places;
        PaddedPODArray<AggregateDataPtr> src_places;
    };
    using DeferredMerges = std::vector<DeferredMergePairs>;

    /// True if the query can benefit from deferring large state merges: some aggregate function
    /// can parallelize its merge and there is a thread pool to run it on.
    bool worthDeferringLargeMerges() const { return has_function_with_parallelizable_merge && params.max_threads > 1; }

    /// Merge the pairs deferred by `mergeDataImpl` / `mergeSingleLevelPartitionImpl` / `mergeStreamsImplCase`,
    /// using the thread pool. With `destroy_sources` the source states are destroyed afterwards (they were
    /// detached from the source hash tables); without it they stay owned by the caller (aggregate columns
    /// of a block of partial states). If `is_cancelled` is set, nothing is merged and only the destruction happens.
    void mergeDeferredLargeStates(DeferredMerges & deferred, Arena * arena, std::atomic<bool> & is_cancelled, bool destroy_sources) const;

    /// The total number of deferred pairs of all aggregate functions.
    static size_t numDeferredMerges(const DeferredMerges & deferred);

    /// Destroy the deferred source states without merging (used on the exception path: the source
    /// hash tables have already been detached from these states, so nothing else will destroy them).
    void destroyDeferredMergeSources(DeferredMerges & deferred) const noexcept;

    /// Merge NULL key data from hash table `src` into `dst`.
    /// `deferred` is the same as for `mergeDataImpl`.
    template <typename Method, typename Table>
    void mergeDataNullKey(
            Table & table_dst,
            Table & table_src,
            Arena * arena,
            DeferredMerges * deferred = nullptr) const;

    /// Merge data from hash table `src` into `dst`.
    /// If `deferred` is not null, pairs of states too large to merge serially are recorded there
    /// (per aggregate function) instead of being merged; the caller must pass them to
    /// `mergeDeferredLargeStates` afterwards.
    template <typename Method, typename Table>
    requires MapAggregationMethod<Method>
    void mergeDataImpl(
        Table & table_dst, Table & table_src, Arena * arena, bool use_compiled_functions, bool prefetch,
        std::atomic<bool> & is_cancelled, const ParallelMergeWorker * parallel_worker = nullptr,
        DeferredMerges * deferred = nullptr)
        const;

    /// Merge the states of one source row `src` into `dst` and destroy them. If `deferred` is not null,
    /// pairs of states too large to merge serially are recorded there instead, as in `mergeDataImpl`.
    void mergeAndDestroyRowOrDefer(AggregateDataPtr dst, AggregateDataPtr src, Arena * arena, DeferredMerges * deferred) const;

    /// Merge the batch of source rows `src_places` into `dst_places` and destroy them, deferring the large pairs to
    /// `deferred` if it is not null. The source rows are detached from their hash table: if it throws, every state
    /// of the batch has been destroyed, except those already recorded in `deferred` and, with `skip_compiled`, the
    /// states of the compiled functions, which the caller has merged by the JIT-compiled function.
    void mergeAndDestroyDetachedBatch(
        PaddedPODArray<AggregateDataPtr> & dst_places,
        PaddedPODArray<AggregateDataPtr> & src_places,
        DeferredMerges * deferred,
        Arena * arena,
        std::atomic<bool> & is_cancelled,
        bool skip_compiled) const;

    /// Whether the state of the `i`-th aggregate function of the source row `src` is the last pair recorded in `deferred`.
    bool isRecordedDeferredPair(const DeferredMerges * deferred, size_t i, ConstAggregateDataPtr src) const;

    /// Merge data from hash table `src` into `dst`, but only for keys that already exist in dst. In other cases, merge the data into `overflows`.
    /// `deferred` is the same as for `mergeDataImpl`.
    template <typename Method, typename Table>
    requires MapAggregationMethod<Method>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena,
        DeferredMerges * deferred = nullptr) const;

    /// A set method has no aggregate states, so there is nothing to merge or overflow.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena,
        DeferredMerges * deferred = nullptr) const;

    /// A set method has no aggregate states: the merge is a plain key union.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
    void mergeDataImpl(
        Table & table_dst, Table & table_src, Arena * arena, bool use_compiled_functions, bool prefetch,
        std::atomic<bool> & is_cancelled, const ParallelMergeWorker * parallel_worker = nullptr,
        DeferredMerges * deferred = nullptr)
        const;

    /// Merge data from hash table `src` into `dst`, but only for keys that already exist in dst. In other cases, merge the data into `overflows`.
    template <typename Method, typename Table>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena,
        DeferredMerges * deferred = nullptr) const;

    /// Same, but ignores the rest of the keys.
    template <typename Method, typename Table>
    requires MapAggregationMethod<Method>
    void mergeDataOnlyExistingKeysImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena,
        DeferredMerges * deferred = nullptr) const;

    /// A set method has no aggregate states, so there is nothing to merge.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
    void mergeDataOnlyExistingKeysImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena,
        DeferredMerges * deferred = nullptr) const;

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

    bool havingPrefilterKeeps(UInt64 count) const
    {
        switch (params.having_prefilter_op)
        {
            case Params::HavingPrefilterOp::Greater: return count > params.having_prefilter_threshold;
            case Params::HavingPrefilterOp::GreaterOrEqual: return count >= params.having_prefilter_threshold;
            case Params::HavingPrefilterOp::Less: return count < params.having_prefilter_threshold;
            case Params::HavingPrefilterOp::LessOrEqual: return count <= params.having_prefilter_threshold;
            case Params::HavingPrefilterOp::Equal: return count == params.having_prefilter_threshold;
            case Params::HavingPrefilterOp::Disabled: return true;
        }
        return true;
    }

    /// `allow_having_prefilter` is sound only on a final conversion of a complete bucket; each caller establishes that.
    /// `untruncated_keys`, when non-null, receives what every group's key - kept or skipped - would occupy
    /// materialized, measured one key at a time on a single reused row, so the accounting stays bounded,
    /// together with a bounded sample of those keys.
    template <typename Method, typename Table>
    requires MapAggregationMethod<Method>
    Chunks convertToBlockImpl(
        Method & method,
        Table & data,
        Arena * arena,
        Arenas & aggregates_pools,
        bool final,
        size_t rows,
        bool return_single_block,
        size_t max_rows_per_block = 0,
        bool allow_having_prefilter = false,
        UntruncatedAggregationKeys * untruncated_keys = nullptr,
        bool keep_table_buffer = false) const;

    /// A set method skips the inline-count and compiled-function paths; it only emits keys.
    template <typename Method, typename Table>
    requires SetAggregationMethod<Method>
    Chunks convertToBlockImpl(
        Method & method,
        Table & data,
        Arena * arena,
        Arenas & aggregates_pools,
        bool final,
        size_t rows,
        bool return_single_block,
        size_t max_rows_per_block = 0,
        bool allow_having_prefilter = false,
        UntruncatedAggregationKeys * untruncated_keys = nullptr,
        bool keep_table_buffer = false) const;

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
        Method & method, Table & data, Arenas & aggregates_pools, bool final, bool return_single_block, size_t max_rows_per_block) const;

    template <typename Method, typename Table>
    Chunks convertToBlockImplFinal(
        Method & method,
        Table & data,
        Arena * arena,
        Arenas & aggregates_pools,
        bool use_compiled_functions,
        bool return_single_block,
        size_t max_rows_per_block,
        bool allow_having_prefilter,
        UntruncatedAggregationKeys * untruncated_keys) const;

    template <typename Method, typename Table>
    Chunks
    convertToBlockImplNotFinal(Method & method, Table & data, Arenas & aggregates_pools, size_t rows, bool return_single_block, size_t max_rows_per_block) const;

    /// `untruncated_keys`, when non-null and the bucket goes through a conversion that materializes
    /// only some of its groups (the Top-K one or the HAVING pre-filter), receives the byte size all
    /// of the bucket's keys would occupy materialized: the runtime
    /// dataflow statistics must describe the untruncated aggregation output (it prices the
    /// shipping term of the parallel-replicas plan, where the partial aggregation materializes
    /// every group), so the chunk of a truncated conversion cannot be measured as is.
    /// `full_group_count`, when non-null, receives the bucket table's group count: the group-by
    /// limit must be enforced against the true cardinality, which the chunk's row count
    /// understates when the Top-K conversion truncates it.
    /// `keep_table_buffer` leaves the converted table empty but with its buffer, for a caller that fills it again
    /// (the adaptive merge's units), where an ordinary conversion releases the buffer at once.
    template <typename Method>
    AggregatedChunk convertOneBucketToChunk(
        AggregatedDataVariants & data_variants,
        Method & method,
        Arena * arena,
        bool final,
        Int32 bucket,
        UntruncatedAggregationKeys * untruncated_keys,
        size_t * full_group_count,
        bool keep_table_buffer = false) const;

    /// The dispatch over the two-level variants of the conversion above, with its parameters.
    AggregatedChunk convertOneBucketToChunk(
        AggregatedDataVariants & variants,
        Arena * arena,
        bool final,
        Int32 bucket,
        UntruncatedAggregationKeys * untruncated_keys = nullptr,
        bool keep_table_buffer = false) const;

    /// The bucket-local Top-K conversion (see `Params::bucket_top_k`): materializes only the
    /// bucket's n best cells by their rank count and destroys the rest, so the sorter
    /// upstream receives at most 256 * n candidate rows instead of every group.
    template <typename Method>
    requires MapAggregationMethod<Method>
    AggregatedChunk convertOneBucketToChunkTopK(
        Method & method, Arena * arena, Arenas & pools_for_output, Int32 bucket, UntruncatedAggregationKeys * untruncated_keys, bool keep_table_buffer) const;

    /// `bucket_top_k` ranks groups by an aggregate, so it is never set for a set method, which has no aggregate
    /// functions at all. This overload exists only because the call site tests it at run time.
    template <typename Method>
    requires SetAggregationMethod<Method>
    AggregatedChunk convertOneBucketToChunkTopK(
        Method & method, Arena * arena, Arenas & pools_for_output, Int32 bucket, UntruncatedAggregationKeys * untruncated_keys, bool keep_table_buffer) const;

    /// The bucket-local Top-K conversion for a count rank, read as the inline count or as the count state, or for a
    /// rank finalized from a `uniqExact` or `uniqExactIf` state (see `bucket_top_k_rank`). The two count readings
    /// differ only in branches outside the scan loops, so they share an instantiation: a third one per method would
    /// grow the binary for no faster scan.
    template <bool finalized_rank, typename Method>
    requires MapAggregationMethod<Method>
    AggregatedChunk convertOneBucketToChunkTopKImpl(
        Method & method, Arena * arena, Arenas & pools_for_output, Int32 bucket, UntruncatedAggregationKeys * untruncated_keys, bool keep_table_buffer) const;

    /// The rank count of the group with state `place` when `Params::bucket_top_k` ranks by `uniqExact` or
    /// `uniqExactIf`: the aggregate's result, finalized into `scratch`, a `ColumnUInt64` emptied first.
    /// `insertResultInto` leaves the state valid for the merge and the conversion that follow.
    UInt64 finalizeBucketTopKRank(AggregateDataPtr place, IColumn & scratch, Arena * arena) const;

    /// `full_group_count`, when non-null, receives the merged bucket's group count (see
    /// `convertOneBucketToChunk`).
    AggregatedChunk mergeAndConvertOneBucketToChunk(
        ManyAggregatedDataVariants & variants,
        Arena * arena,
        bool final,
        Int32 bucket,
        std::atomic<bool> & is_cancelled,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater,
        size_t * full_group_count,
        std::atomic<int> * threshold_top_k_verdict = nullptr) const;

    /// The top-K threshold merge (see `Params::threshold_top_k`): merges and materializes only
    /// the groups that can rank among the bucket's k best by the ordering aggregate. Returns
    /// nothing when it declines at run time (a method it does not serve, a bucket too small
    /// for the pruning to pay for the setup, or a value distribution the threshold cannot cut;
    /// the latter verdict is shared across the buckets through `threshold_top_k_verdict`), and
    /// the caller falls back to the ordinary merge. Defined in AggregatorThresholdTopK.cpp.
    std::optional<AggregatedChunk> tryMergeAndConvertOneBucketToChunkThresholdTopK(
        ManyAggregatedDataVariants & variants,
        Arena * arena,
        Int32 bucket,
        std::atomic<bool> & is_cancelled,
        std::atomic<int> * threshold_top_k_verdict) const;

    template <typename Method>
    requires MapAggregationMethod<Method>
    std::optional<AggregatedChunk> mergeAndConvertOneBucketToChunkThresholdTopKImpl(
        ManyAggregatedDataVariants & variants,
        Arena * arena,
        Int32 bucket,
        std::atomic<bool> & is_cancelled,
        std::atomic<int> * threshold_top_k_verdict) const;

    /// The threshold merge ranks groups by an aggregate's value, which a set method cannot have.
    /// This overload exists only because the call site tests it at run time.
    template <typename Method>
    requires SetAggregationMethod<Method>
    std::optional<AggregatedChunk> mergeAndConvertOneBucketToChunkThresholdTopKImpl(
        ManyAggregatedDataVariants & variants,
        Arena * arena,
        Int32 bucket,
        std::atomic<bool> & is_cancelled,
        std::atomic<int> * threshold_top_k_verdict) const;

    AggregatedChunk prepareChunkAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const;
    AggregatedChunks prepareChunksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final) const;

    /// A non-zero `max_rows_per_block` caps the size of the emitted chunks below `max_block_size`.
    template <bool return_single_block>
    std::conditional_t<return_single_block, AggregatedChunk, AggregatedChunks>
    prepareChunkAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final, size_t max_rows_per_block = 0) const;

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
        Arena * arena_for_keys,
        DeferredMerges * deferred = nullptr) const;

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
        Arena * arena_for_keys,
        DeferredMerges * deferred = nullptr) const;

    /// `arena_for_keys` used to store serialized aggregation keys (in methods like `serialized`) to save some space.
    /// If not provided, aggregates_pool is used instead. Refer to mergeBlocks() for an usage example.
    /// If `deferred` is not null, pairs of large states are collected there instead of being merged at the end of
    /// the block, so that the caller can merge them together with the large states of other blocks via
    /// `mergeDeferredLargeStates`; the caller must keep the block's columns alive until then.
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
        Arena * arena_for_keys = nullptr,
        DeferredMerges * deferred = nullptr) const;

    /// `for_sub_range` as in `executeImpl`, for `mergeOnBlockSmall`.
    template <bool for_sub_range, typename Method, typename Table>
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
        Arena * arena_for_keys,
        DeferredMerges * deferred = nullptr) const;

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

    /// Arbitration between an external-aggregation spill and the kept-keys cutoff
    /// (see `Params::SharedKeptKeysControl`). Always true when the cutoff is not armed.
    /// May permanently abandon the cutoff, so call it only when the spill would proceed.
    bool spillAllowedUnderKeptKeysCutoff(bool no_more_keys, const AggregatedDataVariants & result) const;

    /// Re-inserts the frozen kept keys, with empty states, into a table just emptied by a spill,
    /// so the remaining rows of those keys keep being aggregated (see
    /// `Params::SharedKeptKeysControl`). No-op unless the table is restricted to the kept keys.
    void reseedKeptKeysAfterSpill(AggregatedDataVariants & result) const;

    void ensureLimitsFixedMapMerge(AggregatedDataVariantsPtr data) const;

    /// Check if data variants use fixed-size hash tables (key8/key16) suitable for parallel merge
    /// at single level.
    bool isTypeFixedSize(const ManyAggregatedDataVariants & data_variants) const;

    /// The columns must be alive while the instructions are used.
    void prepareAggregateInstructions(
        const Columns & columns,
        AggregateColumns & aggregate_columns,
        Columns & materialized_columns,
        AggregateFunctionInstructions & instructions,
        NestedColumnsHolder & nested_columns_holder) const;

    /// The instruction-building tail of `prepareAggregateInstructions`: the combinator
    /// unwrapping (-State, -Array) and the batch wiring for one aggregate whose argument
    /// pointers are already in place. Called directly by the adaptive drain, whose argument
    /// columns are rebuilt from the staged records already in that form.
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
        Arena * arena,
        bool all_places_are_non_null);

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
