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
    bool use_sampling;
    std::shared_ptr<ASTFunction> filter_function;
    ActionsDAGPtr filter_expression;
};

struct MergeTreeDataSelectCache
{
    RangesInDataParts parts_with_ranges;
    MergeTreeDataSelectSamplingData sampling;
    std::unique_ptr<ReadFromMergeTree::IndexStats> index_stats;
    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    bool use_cache = false;
};

/** Executes SELECT queries on data from the merge tree.
  */
class MergeTreeDataSelectExecutor
{
public:
    explicit MergeTreeDataSelectExecutor(const MergeTreeData & data_);

    /** When reading, selects a set of parts that covers the desired range of the index.
      * max_blocks_number_to_read - if not nullptr, do not read all the parts whose right border is greater than max_block in partition.
      */
    using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

    QueryPlanPtr read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        unsigned num_streams,
        QueryProcessingStage::Enum processed_stage,
        const PartitionIdToMaxBlock * max_block_numbers_to_read = nullptr) const;

    QueryPlanPtr readFromParts(
        MergeTreeData::DataPartsVector parts,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot_base,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        UInt64 max_block_size,
        unsigned num_streams,
        const PartitionIdToMaxBlock * max_block_numbers_to_read = nullptr,
        MergeTreeDataSelectCache * cache = nullptr) const;

private:
    const MergeTreeData & data;

    Poco::Logger * log;

    QueryPlanPtr spreadMarkRangesAmongStreams(
        RangesInDataParts && parts,
        ReadFromMergeTree::IndexStatPtr index_stats,
        size_t num_streams,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const SelectQueryInfo & query_info,
        const Names & virt_columns,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        const String & query_id) const;

    /// out_projection - save projection only with columns, requested to read
    QueryPlanPtr spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts,
        ReadFromMergeTree::IndexStatPtr index_stats,
        size_t num_streams,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const SelectQueryInfo & query_info,
        const ActionsDAGPtr & sorting_key_prefix_expr,
        const Names & virt_columns,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        ActionsDAGPtr & out_projection,
        const String & query_id,
        const InputOrderInfoPtr & input_order_info) const;

    QueryPlanPtr spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        ReadFromMergeTree::IndexStatPtr index_stats,
        size_t num_streams,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const SelectQueryInfo & query_info,
        const Names & virt_columns,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        ActionsDAGPtr & out_projection,
        const String & query_id) const;

    /// Get the approximate value (bottom estimate - only by full marks) of the number of rows falling under the index.
    size_t getApproximateTotalRowsToRead(
        const MergeTreeData::DataPartsVector & parts,
        const StorageMetadataPtr & metadata_snapshot,
        const KeyCondition & key_condition,
        const Settings & settings) const;

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
        const std::unordered_set<String> & part_values,
        const std::optional<KeyCondition> & minmax_idx_condition,
        const DataTypes & minmax_columns_types,
        std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        PartFilterCounters & counters);

    /// Same as previous but also skip parts uuids if any to the query context, or skip parts which uuids marked as excluded.
    void selectPartsToReadWithUUIDFilter(
        MergeTreeData::DataPartsVector & parts,
        const std::unordered_set<String> & part_values,
        const std::optional<KeyCondition> & minmax_idx_condition,
        const DataTypes & minmax_columns_types,
        std::optional<PartitionPruner> & partition_pruner,
        const PartitionIdToMaxBlock * max_block_numbers_to_read,
        ContextPtr query_context,
        PartFilterCounters & counters) const;
};

}
