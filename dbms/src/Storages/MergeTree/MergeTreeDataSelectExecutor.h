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
    MergeTreeDataSelectExecutor(const MergeTreeData & data_);

    /** When reading, selects a set of parts that covers the desired range of the index.
      * max_blocks_number_to_read - if not nullptr, do not read all the parts whose right border is greater than max_block in partition.
      */
    using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        UInt64 max_block_size,
        unsigned num_streams,
        const PartitionIdToMaxBlock * max_block_numbers_to_read = nullptr) const;

    BlockInputStreams readFromParts(
        MergeTreeData::DataPartsVector parts,
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        UInt64 max_block_size,
        unsigned num_streams,
        const PartitionIdToMaxBlock * max_block_numbers_to_read = nullptr) const;

private:
    const MergeTreeData & data;

    Logger * log;

    BlockInputStreams spreadMarkRangesAmongStreams(
        RangesInDataParts && parts,
        size_t num_streams,
        const Names & column_names,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const PrewhereInfoPtr & prewhere_info,
        const Names & virt_columns,
        const Settings & settings) const;

    BlockInputStreams spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        const Names & column_names,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const PrewhereInfoPtr & prewhere_info,
        const Names & virt_columns,
        const Settings & settings) const;

    /// Get the approximate value (bottom estimate - only by full marks) of the number of rows falling under the index.
    size_t getApproximateTotalRowsToRead(
        const MergeTreeData::DataPartsVector & parts,
        const KeyCondition & key_condition,
        const Settings & settings) const;

    /// Create the expression "Sign == 1".
    void createPositiveSignCondition(
        ExpressionActionsPtr & out_expression,
        String & out_column,
        const Context & context) const;

    MarkRanges markRangesFromPKRange(
        const MergeTreeData::DataPartPtr & part,
        const KeyCondition & key_condition,
        const Settings & settings) const;

    MarkRanges filterMarksUsingIndex(
        MergeTreeIndexPtr index,
        IndexConditionPtr condition,
        MergeTreeData::DataPartPtr part,
        const MarkRanges & ranges,
        const Settings & settings) const;
};

}
