#pragma once

#include <Core/QueryProcessingStage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>


namespace DB
{

class KeyCondition;
struct QueryIdHolder;

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

/** Executes SELECT queries on data from the merge tree.
  */
class MergeTreeDataSelectExecutor
{
public:
    explicit MergeTreeDataSelectExecutor(const MergeTreeData & data_);

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
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr,
        bool enable_parallel_reading = false) const;

    /// The same as read, but with specified set of parts.
    QueryPlanStepPtr readFromParts(
        MergeTreeData::DataPartsVector parts,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        size_t num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr,
        ReadFromMergeTree::AnalysisResultPtr merge_tree_select_result_ptr = nullptr,
        bool enable_parallel_reading = false) const;

    /// Get an estimation for the number of marks we are going to read.
    /// Reads nothing. Secondary indexes are not used.
    /// This method is used to select best projection for table.
    ReadFromMergeTree::AnalysisResultPtr estimateNumMarksToRead(
        MergeTreeData::DataPartsVector parts,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        size_t num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr) const;

    static MarkRanges markRangesFromPKRange(
        const MergeTreeData::DataPartPtr & part,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const std::optional<KeyCondition> & part_offset_condition,
        MarkRanges * exact_ranges,
        const Settings & settings,
        LoggerPtr log);

private:
    const MergeTreeData & data;
    LoggerPtr log;

    /// Get the approximate value (bottom estimate - only by full marks) of the number of rows falling under the index.
    static size_t getApproximateTotalRowsToRead(
        const MergeTreeData::DataPartsVector & parts,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const Settings & settings,
        LoggerPtr log);

    static MarkRanges filterMarksUsingIndex(
        MergeTreeIndexPtr index_helper,
        MergeTreeIndexConditionPtr condition,
        MergeTreeData::DataPartPtr part,
        const MarkRanges & ranges,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        MarkCache * mark_cache,
        UncompressedCache * uncompressed_cache,
        LoggerPtr log);

    static MarkRanges filterMarksUsingMergedIndex(
        MergeTreeIndices indices,
        MergeTreeIndexMergedConditionPtr condition,
        MergeTreeData::DataPartPtr part,
        const MarkRanges & ranges,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        MarkCache * mark_cache,
        UncompressedCache * uncompressed_cache,
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
    static void selectPartsToRead(
        MergeTreeData::DataPartsVector & parts,
        const std::optional<std::unordered_set<String>> & part_values,
        const std::optional<KeyCondition> & minmax_idx_condition,
        const DataTypes & minmax_columns_types,
        const std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        PartFilterCounters & counters,
        QueryStatusPtr query_status);

    /// Same as previous but also skip parts uuids if any to the query context, or skip parts which uuids marked as excluded.
    static void selectPartsToReadWithUUIDFilter(
        MergeTreeData::DataPartsVector & parts,
        const std::optional<std::unordered_set<String>> & part_values,
        MergeTreeData::PinnedPartUUIDsPtr pinned_part_uuids,
        const std::optional<KeyCondition> & minmax_idx_condition,
        const DataTypes & minmax_columns_types,
        const std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        ContextPtr query_context,
        PartFilterCounters & counters,
        LoggerPtr log);

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
        size_t rows_setting,
        size_t bytes_setting,
        size_t rows_granularity,
        size_t bytes_granularity,
        size_t max_marks);

    /// If possible, construct optional key condition from predicates containing _part_offset column.
    static void buildKeyConditionFromPartOffset(
        std::optional<KeyCondition> & part_offset_condition, const ActionsDAG * filter_dag, ContextPtr context);

    /// If possible, filter using expression on virtual columns.
    /// Example: SELECT count() FROM table WHERE _part = 'part_name'
    /// If expression found, return a set with allowed part names (std::nullopt otherwise).
    static std::optional<std::unordered_set<String>> filterPartsByVirtualColumns(
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData & data,
        const MergeTreeData::DataPartsVector & parts,
        const ActionsDAG * filter_dag,
        ContextPtr context);

    /// Filter parts using minmax index and partition key.
    static void filterPartsByPartition(
        MergeTreeData::DataPartsVector & parts,
        const std::optional<PartitionPruner> & partition_pruner,
        const std::optional<KeyCondition> & minmax_idx_condition,
        const std::optional<std::unordered_set<String>> & part_values,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData & data,
        const ContextPtr & context,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        LoggerPtr log,
        ReadFromMergeTree::IndexStats & index_stats);

    /// Filter parts using primary key and secondary indexes.
    /// For every part, select mark ranges to read.
    /// If 'check_limits = true' it will throw exception if the amount of data exceed the limits from settings.
    static RangesInDataParts filterPartsByPrimaryKeyAndSkipIndexes(
        MergeTreeData::DataPartsVector && parts,
        StorageMetadataPtr metadata_snapshot,
        const ContextPtr & context,
        const KeyCondition & key_condition,
        const std::optional<KeyCondition> & part_offset_condition,
        const UsefulSkipIndexes & skip_indexes,
        const MergeTreeReaderSettings & reader_settings,
        LoggerPtr log,
        size_t num_streams,
        ReadFromMergeTree::IndexStats & index_stats,
        bool use_skip_indexes,
        bool find_exact_ranges);

    /// Create expression for sampling.
    /// Also, calculate _sample_factor if needed.
    /// Also, update key condition with selected sampling range.
    static MergeTreeDataSelectSamplingData getSampling(
        const SelectQueryInfo & select_query_info,
        NamesAndTypesList available_real_columns,
        const MergeTreeData::DataPartsVector & parts,
        KeyCondition & key_condition,
        const MergeTreeData & data,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        LoggerPtr log);

    /// Check query limits: max_partitions_to_read, max_concurrent_queries.
    /// Also, return QueryIdHolder. If not null, we should keep it until query finishes.
    static std::shared_ptr<QueryIdHolder> checkLimits(
        const MergeTreeData & data,
        const ReadFromMergeTree::AnalysisResult & result,
        const ContextPtr & context);
};

}
