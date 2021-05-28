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

struct MergeTreeDataSelectSamplingData
{
    bool use_sampling = false;
    bool read_nothing = false;
    Float64 used_sample_factor = 1.0;
    std::shared_ptr<ASTFunction> filter_function;
    ActionsDAGPtr filter_expression;
};

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
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        unsigned num_streams,
        QueryProcessingStage::Enum processed_stage,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr) const;

    size_t estimateNumMarksToRead(
        MergeTreeData::DataPartsVector parts,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot_base,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        unsigned num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr) const;

    QueryPlanPtr readFromParts(
        MergeTreeData::DataPartsVector parts,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot_base,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        unsigned num_streams,
        std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read = nullptr) const;

private:
    const MergeTreeData & data;
    Poco::Logger * log;

    /// Get the approximate value (bottom estimate - only by full marks) of the number of rows falling under the index.
    static size_t getApproximateTotalRowsToRead(
        const MergeTreeData::DataPartsVector & parts,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const Settings & settings,
        Poco::Logger * log);

    static MarkRanges markRangesFromPKRange(
        const MergeTreeData::DataPartPtr & part,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const Settings & settings,
        Poco::Logger * log);

    static MarkRanges filterMarksUsingIndex(
        MergeTreeIndexPtr index_helper,
        MergeTreeIndexConditionPtr condition,
        MergeTreeData::DataPartPtr part,
        const MarkRanges & ranges,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        size_t & total_granules,
        size_t & granules_dropped,
        Poco::Logger * log);

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
        std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        PartFilterCounters & counters);

    /// Same as previous but also skip parts uuids if any to the query context, or skip parts which uuids marked as excluded.
    static void selectPartsToReadWithUUIDFilter(
        MergeTreeData::DataPartsVector & parts,
        const std::optional<std::unordered_set<String>> & part_values,
        MergeTreeData::PinnedPartUUIDsPtr pinned_part_uuids,
        const std::optional<KeyCondition> & minmax_idx_condition,
        const DataTypes & minmax_columns_types,
        std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        ContextPtr query_context,
        PartFilterCounters & counters,
        Poco::Logger * log);

public:
    static size_t roundRowsOrBytesToMarks(
        size_t rows_setting,
        size_t bytes_setting,
        size_t rows_granularity,
        size_t bytes_granularity);

    static size_t minMarksForConcurrentRead(
        size_t rows_setting,
        size_t bytes_setting,
        size_t rows_granularity,
        size_t bytes_granularity,
        size_t max_marks);

    static std::optional<std::unordered_set<String>> filterPartsByVirtualColumns(
        const MergeTreeData & data,
        MergeTreeData::DataPartsVector & parts,
        const ASTPtr & query,
        ContextPtr context);

    static void filterPartsByPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData & data,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        const ContextPtr & query_context,
        MergeTreeData::DataPartsVector & parts,
        const std::optional<std::unordered_set<String>> & part_values,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        Poco::Logger * log,
        ReadFromMergeTree::IndexStats & index_stats);

    static RangesInDataParts filterPartsByPrimaryKeyAndSkipIndexes(
        MergeTreeData::DataPartsVector && parts,
        StorageMetadataPtr metadata_snapshot,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        KeyCondition & key_condition,
        const MergeTreeReaderSettings & reader_settings,
        Poco::Logger * log,
        size_t num_streams,
        ReadFromMergeTree::IndexStats & index_stats,
        bool use_skip_indexes);

    static MergeTreeDataSelectSamplingData getSampling(
        const ASTSelectQuery & select,
        MergeTreeData::DataPartsVector & parts,
        const StorageMetadataPtr & metadata_snapshot,
        KeyCondition & key_condition,
        const MergeTreeData & data,
        Poco::Logger * log,
        bool sample_factor_column_queried,
        NamesAndTypesList available_real_columns,
        ContextPtr context);

    static String checkLimits(
        const MergeTreeData & data,
        const RangesInDataParts & parts_with_ranges,
        const ContextPtr & context);
};

}
