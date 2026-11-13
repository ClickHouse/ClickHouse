#pragma once

#include <Storages/MergeTree/ConditionTemplate.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>

#include <boost/dynamic_bitset.hpp>

#include <expected>
#include <functional>
#include <span>

struct PreformattedMessage;

namespace DB
{

class KeyCondition;
class VectorSimilarityIndexCache;
struct ProjectionDescription;
using ProjectionDescriptionRawPtr = const ProjectionDescription *;

/** Executes SELECT queries on data from the merge tree.
  */
class MergeTreeDataSelectExecutor
{
public:
    using PartialDisjunctionResult = boost::dynamic_bitset<>;

    explicit MergeTreeDataSelectExecutor(const MergeTreeData & data_, ProjectionDescriptionRawPtr projection_ = nullptr);

    /** When reading, selects a set of parts that covers the desired range of the index.
      * max_blocks_number_to_read - if not nullptr, do not read all the parts whose right border is greater than max_block in partition.
      */

    QueryPlanPtr read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        size_t num_streams,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read = nullptr,
        bool enable_parallel_reading = false) const;

    /// The same as read, but with specified set of parts.
    QueryPlanStepPtr readFromParts(
        RangesInDataPartsPtr parts,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        size_t num_streams,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read = nullptr,
        ReadFromMergeTree::AnalysisResultPtr merge_tree_select_result_ptr = nullptr,
        bool enable_parallel_reading = false,
        std::shared_ptr<ParallelReadingExtension> extension_ = nullptr) const;

    /// Get an estimation for the number of marks we are going to read.
    /// Reads nothing. Secondary indexes are not used.
    /// This method is used to select best projection for table.
    /// `top_k_filter_info` is the TopK stamp of the read this analysis is performed for (if any):
    /// the query condition cache consult inside must observe the same TopK gating and key salting
    /// as the read itself, otherwise a projection candidate would be analyzed as an apparent plain
    /// read and could reuse plain `SELECT ... WHERE` entries that
    /// `use_query_condition_cache_for_top_k = 0` is supposed to gate off.
    /// A caller which cannot know the TopK stamp of the read - because the estimate runs before
    /// `tryOptimizeTopK` - must pass `use_query_condition_cache = false` instead, so that the estimate
    /// neither consults nor populates the cache under the plain condition hash.
    ReadFromMergeTree::AnalysisResultPtr estimateNumMarksToRead(
        RangesInDataParts parts,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const std::optional<TopKFilterInfo> & top_k_filter_info,
        ContextPtr context,
        size_t num_streams,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read = nullptr,
        bool use_query_condition_cache = true,
        /// Whether the query condition cache entries a TopK read writes under its `PREWHERE` key may
        /// be consulted here. Projection candidate analysis is the only place a warm query consults
        /// the cache before the plan is final, so it has to pass the gate of the read it analyses;
        /// every other caller runs before `tryOptimizeTopK` and keeps the conservative default.
        bool allow_top_k_prewhere_query_condition_cache = false) const;

    static MarkRanges markRangesFromPKRange(
        const MergeTreeData::DataPartPtr & part,
        const MarkRanges & part_ranges,
        size_t part_starting_offset_in_query,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const KeyCondition * part_offset_condition,
        const KeyCondition * total_offset_condition,
        MarkRanges * exact_ranges,
        const std::vector<std::optional<size_t>> * pk_to_minmax_slot,
        const Settings & settings,
        LoggerPtr log);

    /// Maps each primary-key column position to the slot of the matching column in a part's partition
    /// minmax index (nullopt when the primary-key column is not a partition-minmax column).
    static std::vector<std::optional<size_t>> buildPrimaryKeyToMinMaxSlotMapping(
        const StorageMetadataPtr & metadata_snapshot, const MergeTreeSettingsPtr & data_settings);

    /// Works on any part description, not only on an `IMergeTreeDataPart`.
    static MarkRanges markRangesFromPKRange(
        const IMergeTreeDataPartInfoForReader & part,
        const MarkRanges & part_ranges,
        size_t part_starting_offset_in_query,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const KeyCondition * part_offset_condition,
        const KeyCondition * total_offset_condition,
        MarkRanges * exact_ranges,
        const std::vector<std::optional<size_t>> * pk_to_minmax_slot,
        const Settings & settings,
        LoggerPtr log);

    static MarkRanges markRangesFromPKRange(
        const RangesInDataPart & part_with_ranges,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const KeyCondition * part_offset_condition,
        const KeyCondition * total_offset_condition,
        MarkRanges * exact_ranges,
        const std::vector<std::optional<size_t>> * pk_to_minmax_slot,
        const Settings & settings,
        LoggerPtr log);

    static std::pair<MarkRanges, RangesInDataPartReadHints> filterMarksUsingIndex(
        MergeTreeIndexPtr index_helper,
        MergeTreeIndexConditionPtr condition,
        const KeyCondition * key_condition_rpn_template,
        const MergeTreeDataPartInfoForReaderPtr & part_info,
        const MarkRanges & ranges,
        const RangesInDataPartReadHints & in_read_hints,
        const MergeTreeReaderSettings & reader_settings,
        MarkCache * mark_cache,
        UncompressedCache * uncompressed_cache,
        VectorSimilarityIndexCache * vector_similarity_index_cache,
        bool use_skip_indexes_for_disjunctions,
        PartialDisjunctionResult & partial_disjunction_result,
        LoggerPtr log);

    static MergeTreeIndexBulkGranulesMinMaxPtr getMinMaxIndexGranules(
        const MergeTreeDataPartInfoForReaderPtr & part_info,
        MergeTreeIndexPtr skip_index_minmax,
        const MarkRanges & ranges,
        int direction,
        bool access_by_mark,
        const MergeTreeReaderSettings & reader_settings,
        MarkCache * mark_cache,
        UncompressedCache * uncompressed_cache,
        VectorSimilarityIndexCache * vector_similarity_index_cache);

    /// Whether evaluation of OR-connected filter conditions using skip indexes (setting `use_skip_indexes_for_disjunctions`)
    /// may be used for a filter whose RPN has `rpn_size` elements (setting `use_skip_indexes_for_disjunctions_max_rpn_elements`).
    /// One bit per RPN element per mark is consumed to store the partial evaluation results.
    /// Example:
    /// - (A = 5 OR B = 5) requires 3 bits - for A, for B and for OR(A, B).
    /// - Likewise, (A = 5 OR B = 5) AND (C > 5 OR D > 5) requires 7 bits.
    static bool isRPNSizeAllowedForSkipIndexesForDisjunctions(size_t rpn_size, const Settings & settings);

    /// Returns whether OR-connected filters are evaluated with skip indexes during index analysis.
    static bool useSkipIndexesForDisjunctions(
        bool requested,
        bool use_skip_indexes_on_data_read,
        const ConditionTemplate<KeyCondition> * key_condition_rpn_template,
        const Settings & settings);

private:
    const MergeTreeData & data;
    MergeTreeSettingsPtr data_settings;
    LoggerPtr log;

    /// Get the approximate value (bottom estimate - only by full marks) of the number of rows falling under the index.
    static size_t getApproximateTotalRowsToRead(
        const RangesInDataParts & parts,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const Settings & settings,
        LoggerPtr log);

    struct PartFilterCounters
    {
        size_t num_initial_selected_parts = 0;
        size_t num_initial_selected_granules = 0;
        size_t num_parts_after_minmax = 0;
        size_t num_granules_after_minmax = 0;
        size_t num_parts_after_partition_pruner = 0;
        size_t num_granules_after_partition_pruner = 0;
    };

    /// Select the parts in which there can be data that satisfy `minmax_idx_condition` and that match the condition on `_part`,
    ///  as well as `max_block_number_to_read`.
    static RangesInDataParts selectPartsToRead(
        const RangesInDataParts & parts,
        const std::optional<std::unordered_set<String>> & part_values,
        const ConditionTemplate<KeyCondition>::Ptr & minmax_idx_condition,
        const DataTypes & minmax_columns_types,
        const std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        PartFilterCounters & counters,
        QueryStatusPtr query_status);

public:
    /// For given number rows and bytes, get the number of marks to read.
    /// It is a minimal number of marks which contain so many rows and bytes.
    static size_t roundRowsOrBytesToMarks(
        size_t rows_setting,
        size_t bytes_setting,
        size_t rows_granularity,
        size_t bytes_granularity);

    /// The same as roundRowsOrBytesToMarks, but return no more than max_marks.
    static size_t minMarksForConcurrentRead(
        size_t rows_setting, size_t bytes_setting, size_t rows_granularity, size_t bytes_granularity, size_t min_marks, size_t max_marks);

    /// If possible, construct optional key condition template from predicates containing _part_offset and _part column.
    static ConditionTemplate<KeyCondition>::Ptr buildKeyConditionFromPartOffset(
        const std::shared_ptr<ActionsDAGWithInversionPushDown> & filter_dag,
        const StorageMetadataPtr & metadata_snapshot,
        bool skip_folding,
        ContextPtr context);

    /// If possible, construct optional key condition template from predicates containing _part_offset + _part_starting_offset expression.
    static ConditionTemplate<KeyCondition>::Ptr buildKeyConditionFromTotalOffset(
        const std::shared_ptr<ActionsDAGWithInversionPushDown> & filter_dag,
        const StorageMetadataPtr & metadata_snapshot,
        bool skip_folding,
        ContextPtr context);

    /// If possible, filter using expression on virtual columns.
    /// Example: SELECT count() FROM table WHERE _part = 'part_name'
    /// If expression found, return a set with allowed part names (std::nullopt otherwise).
    static std::optional<std::unordered_set<String>> filterPartsByVirtualColumns(
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData & data,
        const RangesInDataParts & parts,
        const ActionsDAG::Node * predicate,
        ContextPtr context);

    /// Loading statistics costs a few reads per data part, so they pay off only for a read of at least
    /// `min_marks_per_part` marks for each part whose statistics are not in memory yet.
    /// Returns the number of marks to compare with the read, zero if no statistics would be loaded.
    static size_t getMinMarksToLoadStatistics(size_t min_marks_per_part, size_t parts_to_load_statistics);

    /// Whether fewer than `limit` marks of the parts are left to read after the primary key analysis.
    /// Stops the analysis as soon as `limit` marks are found, and bounds the analysis of each part,
    /// so the answer is false when it cannot be established cheaply. Always false if `limit` is zero.
    static bool hasFewerMarksToReadThan(
        const RangesInDataParts & parts,
        const StorageMetadataPtr & metadata_snapshot,
        const ConditionTemplate<KeyCondition>::Ptr & key_condition,
        size_t limit,
        const Settings & settings,
        LoggerPtr log);

    /// Apply snapshot, virtual-column, min-max and partition filters before range analysis.
    static RangesInDataParts filterParts(
        const RangesInDataParts & parts,
        const ReadFromMergeTree::Indexes & indexes,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData & data,
        const ContextPtr & context,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        LoggerPtr log,
        ReadFromMergeTree::IndexStats & index_stats);

    /// Filter parts using minmax index and partition key.
    static RangesInDataParts filterPartsByPartition(
        const RangesInDataParts & parts,
        const std::optional<PartitionPruner> & partition_pruner,
        const ConditionTemplate<KeyCondition>::Ptr & minmax_idx_condition,
        const std::optional<std::unordered_set<String>> & part_values,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData & data,
        const ContextPtr & context,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        LoggerPtr log,
        ReadFromMergeTree::IndexStats & index_stats,
        bool check_index_usage = true);

    /// Filter parts using column statistics.
    /// Returns filtered parts and updates index_stats with statistics pruning info.
    /// Parts whose statistics are in memory are always pruned. Loading statistics costs a few reads per part,
    /// so the other parts are not pruned if `key_condition` leaves fewer than `min_marks_per_part_to_load_statistics`
    /// marks to read in them per part (see `getMinMarksToLoadStatistics` and `hasFewerMarksToReadThan`).
    /// If `deferred_parts` is not nullptr, the parts left unpruned for that reason are written to it.
    static RangesInDataParts filterPartsByStatistics(
        const RangesInDataParts & parts,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
        const ConditionTemplate<KeyCondition>::Ptr & key_condition,
        size_t min_marks_per_part_to_load_statistics,
        const ContextPtr & context,
        LoggerPtr log,
        ReadFromMergeTree::IndexStats & index_stats,
        RangesInDataParts * deferred_parts = nullptr);

    struct IndexAnalysisContext
    {
        StorageMetadataPtr metadata_snapshot;
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
        const SelectQueryInfo & query_info;
        const ContextPtr & context;
        const ReadFromMergeTree::Indexes & indexes;
        const std::optional<TopKFilterInfo> & top_k_filter_info;
        const MergeTreeReaderSettings & reader_settings;
        LoggerPtr log;
        size_t num_streams;
        bool find_exact_ranges;
        bool is_parallel_reading_from_replicas;
        bool has_projections;
        bool check_row_limits;
        bool use_primary_key = true;
        ReadFromMergeTree::AnalysisResult & result;
    };

    /// Counters of one index in one analysis.
    struct IndexAnalysisStat
    {
        std::atomic<size_t> total_granules = 0;
        std::atomic<size_t> granules_dropped = 0;
        std::atomic<size_t> total_parts = 0;
        std::atomic<size_t> parts_dropped = 0;
        std::atomic<size_t> elapsed_us = 0;
        std::atomic<MarkRanges::SearchAlgorithm> search_algorithm = MarkRanges::SearchAlgorithm::Unknown;
    };

    struct IndexAnalysisStats
    {
        explicit IndexAnalysisStats(size_t num_skip_index_stats) : skip_indexes(num_skip_index_stats) {}

        IndexAnalysisStat primary_key;
        /// By position in the part's skip index order; one block per part with `per_part_index_stats`.
        std::vector<IndexAnalysisStat> skip_indexes;
        std::atomic<size_t> marks_after_primary_key = 0;
        std::atomic<size_t> parts_after_primary_key = 0;
        std::atomic<size_t> marks_after_disjunctions = 0;
    };

    /// Query-level inputs of `analyzeSinglePart`, shared by all parts and threads.
    struct SinglePartIndexAnalysisParams
    {
        StorageMetadataPtr metadata_snapshot;
        ContextPtr context;
        const Settings & settings;
        const MergeTreeReaderSettings & reader_settings;
        LoggerPtr log;

        bool use_primary_key = true;
        const ConditionTemplate<KeyCondition> & key_condition;
        const ConditionTemplate<KeyCondition> * part_offset_condition = nullptr;
        /// Only with the real `part_starting_offset_in_query` of every part.
        const ConditionTemplate<KeyCondition> * total_offset_condition = nullptr;
        const std::vector<std::optional<size_t>> * pk_to_minmax_slot = nullptr;
        bool find_exact_ranges = false;
        /// `FINAL` exact mode.
        bool keep_ranges_after_primary_key = false;

        const UsefulSkipIndexes & skip_indexes;
        const ConditionTemplate<KeyCondition> * key_condition_rpn_template = nullptr;
        /// The result of `useSkipIndexesForDisjunctions`.
        bool use_skip_indexes_for_disjunctions = false;
        bool use_skip_indexes_on_data_read = false;
        bool query_hides_lightweight_deleted_rows = true;
        MarkCache * mark_cache = nullptr;
        UncompressedCache * uncompressed_cache = nullptr;
        VectorSimilarityIndexCache * vector_similarity_index_cache = nullptr;
    };

    /// References into the caller's per-part state.
    struct PartRangesForIndexAnalysis
    {
        MarkRanges & ranges;
        RangesInDataPartReadHints & read_hints;
        MarkRanges & exact_ranges;
        std::optional<MarkRanges> & ranges_snapshot_after_pk_analysis;
        size_t part_starting_offset_in_query = 0;
    };

    /// Primary key and offset conditions, then the skip indexes in `skip_index_order` (declaration order when null), then the
    /// disjunction merge. Calls `get_part_for_skip_indexes` once, only when skip indexes run, inside the
    /// `FilteringMarksWithSecondaryKeys` metric scope; it returns the same part with its alter conversions (not null).
    /// Per-index counters go to `skip_index_stats` (the caller's slice of
    /// `IndexAnalysisStats::skip_indexes`, by position in the order).
    /// Returns whether a skip index is usable for the part, applied here or deferred to the read.
    static bool analyzeSinglePart(
        const SinglePartIndexAnalysisParams & params,
        const IMergeTreeDataPartInfoForReader & part,
        const std::function<MergeTreeDataPartInfoForReaderPtr()> & get_part_for_skip_indexes,
        const std::vector<size_t> * skip_index_order,
        PartRangesForIndexAnalysis ranges,
        IndexAnalysisStats & stats,
        std::span<IndexAnalysisStat> skip_index_stats);

    /// Filter parts using primary key and secondary indexes.
    /// For every part, select mark ranges to read.
    static RangesInDataParts filterPartsByPrimaryKeyAndSkipIndexes(IndexAnalysisContext & filter_context, RangesInDataParts parts_with_ranges, ReadFromMergeTree::IndexStats & index_stats);

    /// Throws if an index in `force_data_skipping_indices` is not among the useful ones.
    static void checkForcedSkipIndexes(const UsefulSkipIndexes & skip_indexes, const Settings & settings);

    /// Filter parts using query condition cache.
    static void filterPartsByQueryConditionCache(
        RangesInDataParts & parts_with_ranges,
        const SelectQueryInfo & select_query_info,
        const std::optional<VectorSearchParameters> & vector_search_parameters,
        const std::optional<TopKFilterInfo> & top_k_filter_info,
        bool allow_top_k_prewhere_query_condition_cache,
        bool use_sampling,
        const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
        const ReadFromMergeTree::Indexes & indexes,
        const ContextPtr & context,
        LoggerPtr log);

    /// Salt a WHERE/PREWHERE condition hash with the effective skip-index profile that index
    /// analysis actually ran: the set of useful skip indexes (their names and types, after
    /// use_skip_indexes / ignore_data_skipping_indices / metadata have been applied) and the
    /// effective use_skip_indexes_for_disjunctions mode. Skip-index-derived query condition
    /// cache exclusions are stored under this profiled hash so a query that ran a different
    /// set of indexes (e.g. use_skip_indexes = 0, an index dropped/ignored, or a different
    /// disjunction mode) never consults a verdict produced by an index it did not run.
    /// Both the write side (ReadFromMergeTree) and the read side (filterPartsByQueryConditionCache)
    /// pass the same Indexes and the same distributed_index_analysis value, so the salt is identical
    /// on both.
    static UInt64 getSkipIndexProfiledConditionHash(
        UInt64 condition_hash, const ReadFromMergeTree::Indexes & indexes, bool distributed_index_analysis);

    /// Create expression for sampling.
    /// Also, calculate _sample_factor if needed.
    /// Also, update key condition with selected sampling range.
    static MergeTreeDataSelectSamplingData getSampling(
        const SelectQueryInfo & select_query_info,
        NamesAndTypesList available_real_columns,
        const RangesInDataParts & parts,
        ConditionTemplate<KeyCondition>::Ptr & key_condition,
        const MergeTreeData & data,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        LoggerPtr log);

    struct RowLimits
    {
        SizeLimits limits;
        SizeLimits leaf_limits;
    };

    /// Calculate row limits for reading based on settings and query info.
    /// Returns limits and leaf_limits that should be applied during part processing.
    static RowLimits getRowLimits(const Context & context, const SelectQueryInfo & query_info);

    static MarkRanges mergePartialResultsForDisjunctions(
        const IMergeTreeDataPartInfoForReader & part_info,
        const MarkRanges & ranges,
        const KeyCondition & rpn_template_for_eval_result,
        const PartialDisjunctionResult & partial_eval_results,
        MergeTreeReaderSettings reader_settings,
        LoggerPtr log);

    /// Orders the useful indices for one part: vector similarity first, then minmax, then coarser granularity, then smaller index files.
    /// `index_sizes_scratch` is a buffer the caller reuses across parts.
    static SkipIndexOrder computeSkipIndexOrder(
        const IMergeTreeDataPartInfoForReader & part,
        const std::vector<MergeTreeIndexWithCondition> & useful_indices,
        std::vector<size_t> & index_sizes_scratch);

    /// Returns an error if the index depends on a column updated on the fly. A value update
    /// (@value_updated_columns: ALTER UPDATE / patches / pending DROP COLUMN), a pending RENAME COLUMN,
    /// or a MATERIALIZED column the read recalculates from an updated one, always makes it stale. A
    /// value-preserving type change is tolerated when @index_column_types records the granules were built against the
    /// current read type. Per-part callers pass `AlterConversions::getColumnsInvalidatingIndexes` as
    /// @all_updated_columns (it also holds the names touched by a pending DROP/RENAME COLUMN).
    /// @index_column_types is optional: some callers aggregate updated columns across parts and decide once.
    static std::expected<void, PreformattedMessage> canUseIndex(
        const MergeTreeIndexPtr & index,
        const StorageMetadataPtr & metadata_snapshot,
        const NameSet & all_updated_columns,
        const NameSet & value_updated_columns,
        const ContextPtr & context,
        const SecondaryIndexColumnTypes * index_column_types = nullptr);

    /// Whether a pending alter mutation (e.g. `MODIFY COLUMN`) or metadata mutation (`DROP COLUMN`,
    /// `RENAME COLUMN`) applies to the part. The counters of the snapshot (`hasAlterMutations`,
    /// `hasMetadataMutations`) also include finished mutations kept for `system.mutations`, so only the
    /// commands newer than the part are checked.
    static bool partHasPendingAlterOrMetadataMutations(
        const MergeTreeData::DataPartPtr & part,
        const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot);

    /// Same as `partHasPendingAlterOrMetadataMutations` for any of the parts.
    static bool hasPendingAlterOrMetadataMutations(
        const RangesInDataParts & parts,
        const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot);


};

}
