#pragma once

#include <Core/QueryProcessingStage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

class KeyCondition;


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
        const Context & context,
        UInt64 max_block_size,
        unsigned num_streams,
        const PartitionIdToMaxBlock * max_block_numbers_to_read = nullptr) const;

    QueryPlanPtr readFromParts(
        MergeTreeData::DataPartsVector parts,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        UInt64 max_block_size,
        unsigned num_streams,
        const PartitionIdToMaxBlock * max_block_numbers_to_read = nullptr) const;

private:
    const MergeTreeData & data;

    Poco::Logger * log;

    QueryPlanPtr spreadMarkRangesAmongStreams(
        RangesInDataParts && parts,
        size_t num_streams,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const SelectQueryInfo & query_info,
        const Names & virt_columns,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings) const;

    /// out_projection - save projection only with columns, requested to read
    QueryPlanPtr spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts,
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
        ActionsDAGPtr & out_projection) const;

    QueryPlanPtr spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        size_t num_streams,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const SelectQueryInfo & query_info,
        const Names & virt_columns,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        ActionsDAGPtr & out_projection) const;

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
        Poco::Logger * log);
};

}
