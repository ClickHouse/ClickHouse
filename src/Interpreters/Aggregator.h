#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <type_traits>
#include <variant>


#include <base/StringRef.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelStringHashMap.h>

#include <Common/ThreadPool.h>
#include <Common/ColumnsHashing.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>
#include <Core/ColumnNumbers.h>

#include <QueryPipeline/SizeLimits.h>

#include <Disks/SingleDiskVolume.h>
#include <Disks/TemporaryFileOnDisk.h>

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>

#include <Parsers/IAST_fwd.h>

#include <Interpreters/AggregationMethod.h>
#include <Interpreters/AggregatedData.h>
#include <Interpreters/AggregatedDataVariants.h>

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
    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container *>;
    using AggregateColumnsConstData = std::vector<const ColumnAggregateFunction::Container *>;
    using AggregateFunctionsPlainPtrs = std::vector<const IAggregateFunction *>;

    struct Params
    {
        /// What to count.
        const Names keys;
        const AggregateDescriptions aggregates;
        const size_t keys_size;
        const size_t aggregates_size;

        /// The settings of approximate calculation of GROUP BY.
        const bool overflow_row;    /// Do we need to put into AggregatedDataVariants::without_key aggregates for keys that are not in max_rows_to_group_by.
        const size_t max_rows_to_group_by;
        const OverflowMode group_by_overflow_mode;

        /// Two-level aggregation settings (used for a large number of keys).
        /** With how many keys or the size of the aggregation state in bytes,
          *  two-level aggregation begins to be used. Enough to reach of at least one of the thresholds.
          * 0 - the corresponding threshold is not specified.
          */
        size_t group_by_two_level_threshold;
        size_t group_by_two_level_threshold_bytes;

        /// Settings to flush temporary data to the filesystem (external aggregation).
        const size_t max_bytes_before_external_group_by;        /// 0 - do not use external aggregation.

        /// Return empty result when aggregating without keys on empty set.
        bool empty_result_for_aggregation_by_empty_set;

        TemporaryDataOnDiskScopePtr tmp_data_scope;

        /// Settings is used to determine cache size. No threads are created.
        size_t max_threads;

        const size_t min_free_disk_space;

        bool compile_aggregate_expressions;
        size_t min_count_to_compile_aggregate_expression;

        size_t max_block_size;

        bool only_merge;

        bool enable_prefetch;

        bool optimize_group_by_constant_keys;

        const double min_hit_rate_to_use_consecutive_keys_optimization;

        struct StatsCollectingParams
        {
            StatsCollectingParams();

            StatsCollectingParams(
                const ASTPtr & select_query_,
                bool collect_hash_table_stats_during_aggregation_,
                size_t max_entries_for_hash_table_stats_,
                size_t max_size_to_preallocate_for_aggregation_);

            bool isCollectionAndUseEnabled() const { return key != 0; }
            void disable() { key = 0; }

            UInt64 key = 0;
            const size_t max_entries_for_hash_table_stats = 0;
            const size_t max_size_to_preallocate_for_aggregation = 0;
        };

        StatsCollectingParams stats_collecting_params;

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
            double min_hit_rate_to_use_consecutive_keys_optimization_,
            const StatsCollectingParams & stats_collecting_params_)
            : keys(keys_)
            , aggregates(aggregates_)
            , keys_size(keys.size())
            , aggregates_size(aggregates.size())
            , overflow_row(overflow_row_)
            , max_rows_to_group_by(max_rows_to_group_by_)
            , group_by_overflow_mode(group_by_overflow_mode_)
            , group_by_two_level_threshold(group_by_two_level_threshold_)
            , group_by_two_level_threshold_bytes(group_by_two_level_threshold_bytes_)
            , max_bytes_before_external_group_by(max_bytes_before_external_group_by_)
            , empty_result_for_aggregation_by_empty_set(empty_result_for_aggregation_by_empty_set_)
            , tmp_data_scope(std::move(tmp_data_scope_))
            , max_threads(max_threads_)
            , min_free_disk_space(min_free_disk_space_)
            , compile_aggregate_expressions(compile_aggregate_expressions_)
            , min_count_to_compile_aggregate_expression(min_count_to_compile_aggregate_expression_)
            , max_block_size(max_block_size_)
            , only_merge(only_merge_)
            , enable_prefetch(enable_prefetch_)
            , optimize_group_by_constant_keys(optimize_group_by_constant_keys_)
            , min_hit_rate_to_use_consecutive_keys_optimization(min_hit_rate_to_use_consecutive_keys_optimization_)
            , stats_collecting_params(stats_collecting_params_)
        {
        }

        /// Only parameters that matter during merge.
        Params(const Names & keys_, const AggregateDescriptions & aggregates_, bool overflow_row_, size_t max_threads_, size_t max_block_size_, double min_hit_rate_to_use_consecutive_keys_optimization_)
            : Params(
                keys_, aggregates_, overflow_row_, 0, OverflowMode::THROW, 0, 0, 0, false, nullptr, max_threads_, 0, false, 0, max_block_size_, false, true, false, min_hit_rate_to_use_consecutive_keys_optimization_, {})
        {
        }

        static Block
        getHeader(const Block & header, bool only_merge, const Names & keys, const AggregateDescriptions & aggregates, bool final);

        Block getHeader(const Block & header_, bool final) const { return getHeader(header_, only_merge, keys, aggregates, final); }

        /// Remember the columns we will work with
        ColumnRawPtrs makeRawKeyColumns(const Block & block) const;
        AggregateColumnsConstData makeAggregateColumnsData(const Block & block) const;

        /// Returns keys and aggregated for EXPLAIN query
        void explain(WriteBuffer & out, size_t indent) const;
        void explain(JSONBuilder::JSONMap & map) const;
    };

    explicit Aggregator(const Block & header_, const Params & params_);

    /// Process one block. Return false if the processing should be aborted (with group_by_overflow_mode = 'break').
    bool executeOnBlock(const Block & block,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys) const;

    bool executeOnBlock(Columns columns,
        size_t row_begin, size_t row_end,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys) const;

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
    bool mergeOnBlock(Block block,
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
    BlocksList convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads) const;

    ManyAggregatedDataVariants prepareVariantsToMerge(ManyAggregatedDataVariants && data_variants) const;

    using BucketToBlocks = std::map<Int32, BlocksList>;
    /// Merge partially aggregated blocks separated to buckets into one data structure.
    void mergeBlocks(BucketToBlocks bucket_to_blocks, AggregatedDataVariants & result, size_t max_threads, std::atomic<bool> & is_cancelled);

    /// Merge several partially aggregated blocks into one.
    /// Precondition: for all blocks block.info.is_overflows flag must be the same.
    /// (either all blocks are from overflow data or none blocks are).
    /// The resulting block has the same value of is_overflows flag.
    Block mergeBlocks(BlocksList & blocks, bool final, std::atomic<bool> & is_cancelled);

    /** Split block with partially-aggregated data to many blocks, as if two-level method of aggregation was used.
      * This is needed to simplify merging of that data with other results, that are already two-level.
      */
    std::vector<Block> convertBlockToTwoLevel(const Block & block) const;

    /// For external aggregation.
    void writeToTemporaryFile(AggregatedDataVariants & data_variants, size_t max_temp_file_size = 0) const;

    bool hasTemporaryData() const { return tmp_data && !tmp_data->empty(); }

    const TemporaryDataOnDisk & getTemporaryData() const { return *tmp_data; }

    /// Get data structure of the result.
    Block getHeader(bool final) const;

private:

    friend struct AggregatedDataVariants;
    friend class ConvertingAggregatedToChunksTransform;
    friend class ConvertingAggregatedToChunksSource;
    friend class ConvertingAggregatedToChunksWithMergingSource;
    friend class AggregatingInOrderTransform;

    /// Data structure of source blocks.
    Block header;
    /// Positions of aggregation key columns in the header.
    const ColumnNumbers keys_positions;
    Params params;

    AggregatedDataVariants::Type method_chosen;
    Sizes key_sizes;

    HashMethodContextPtr aggregation_state_cache;

    AggregateFunctionsPlainPtrs aggregate_functions;

    using AggregateFunctionInstructions = std::vector<AggregateFunctionInstruction>;
    using NestedColumnsHolder = std::vector<std::vector<const IColumn *>>;

    Sizes offsets_of_aggregate_states;    /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.

    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;

    bool all_aggregates_has_trivial_destructor = false;

    /// How many RAM were used to process the query before processing the first block.
    Int64 memory_usage_before_aggregation = 0;

    LoggerPtr log = getLogger("Aggregator");

    /// For external aggregation.
    TemporaryDataOnDiskPtr tmp_data;

    size_t min_bytes_for_prefetch = 0;

#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledAggregateFunctionsHolder> compiled_aggregate_functions_holder;
#endif

    std::vector<bool> is_aggregate_function_compiled;

    /** Try to compile aggregate functions.
      */
    void compileAggregateFunctionsIfNeeded();

    /** Select the aggregation method based on the number and types of keys. */
    AggregatedDataVariants::Type chooseAggregationMethod();

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

    void executeAggregateInstructions(
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        const std::unique_ptr<AggregateDataPtr[]> & places,
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
        TemporaryFileStream & out) const;

    /// Merge NULL key data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataNullKey(
            Table & table_dst,
            Table & table_src,
            Arena * arena) const;

    /// Merge data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataImpl(Table & table_dst, Table & table_src, Arena * arena, bool use_compiled_functions, bool prefetch) const;

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
        ManyAggregatedDataVariants & non_empty_data) const;

    template <bool return_single_block>
    using ConvertToBlockRes = std::conditional_t<return_single_block, Block, BlocksList>;
    using ConvertToBlockResVariant = std::variant<Block, BlocksList>;

    template <typename Method, typename Table>
    ConvertToBlockResVariant
    convertToBlockImpl(Method & method, Table & data, Arena * arena, Arenas & aggregates_pools, bool final, size_t rows, bool return_single_block) const;

    template <typename Mapped>
    void insertAggregatesIntoColumns(
        Mapped & mapped,
        MutableColumns & final_aggregate_columns,
        Arena * arena) const;

    Block insertResultsIntoColumns(
        PaddedPODArray<AggregateDataPtr> & places,
        OutputBlockColumns && out_cols,
        Arena * arena,
        bool has_null_key_data,
        bool use_compiled_functions) const;

    template <typename Method, typename Table>
    ConvertToBlockResVariant convertToBlockImplFinal(
        Method & method,
        Table & data,
        Arena * arena,
        Arenas & aggregates_pools,
        bool use_compiled_functions,
        bool return_single_block) const;

    template <typename Method, typename Table>
    ConvertToBlockResVariant
    convertToBlockImplNotFinal(Method & method, Table & data, Arenas & aggregates_pools, size_t rows, bool return_single_block) const;

    template <typename Method>
    Block convertOneBucketToBlock(
        AggregatedDataVariants & data_variants,
        Method & method,
        Arena * arena,
        bool final,
        Int32 bucket) const;

    Block convertOneBucketToBlock(AggregatedDataVariants & variants, Arena * arena, bool final, Int32 bucket) const;

    Block mergeAndConvertOneBucketToBlock(
        ManyAggregatedDataVariants & variants,
        Arena * arena,
        bool final,
        Int32 bucket,
        std::atomic<bool> & is_cancelled) const;

    Block prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const;
    BlocksList prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, ThreadPool * thread_pool) const;

    template <bool return_single_block>
    ConvertToBlockRes<return_single_block> prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const;

    template <typename Method>
    BlocksList prepareBlocksAndFillTwoLevelImpl(
        AggregatedDataVariants & data_variants,
        Method & method,
        bool final,
        ThreadPool * thread_pool) const;

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
        Arena * arena_for_keys) const;

    /// `arena_for_keys` used to store serialized aggregation keys (in methods like `serialized`) to save some space.
    /// If not provided, aggregates_pool is used instead. Refer to mergeBlocks() for an usage example.
    template <typename Method, typename Table>
    void mergeStreamsImpl(
        Block block,
        Arena * aggregates_pool,
        Method & method,
        Table & data,
        AggregateDataPtr overflow_row,
        LastElementCacheStats & consecutive_keys_cache_stats,
        bool no_more_keys,
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
        Arena * arena_for_keys) const;

    void mergeBlockWithoutKeyStreamsImpl(
        Block block,
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
        const Block & source,
        std::vector<Block> & destinations) const;

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


struct HashTablesCacheStatistics
{
    size_t entries = 0;
    size_t hits = 0;
    size_t misses = 0;
};

std::optional<HashTablesCacheStatistics> getHashTablesCacheStatistics();
}
