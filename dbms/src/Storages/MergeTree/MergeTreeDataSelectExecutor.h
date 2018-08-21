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
    MergeTreeDataSelectExecutor(MergeTreeData & data_);

    /** When reading, selects a set of parts that covers the desired range of the index.
      * max_block_number_to_read - if not zero, do not read all the parts whose right border is greater than this threshold.
      */
    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        size_t max_block_size,
        unsigned num_streams,
        Int64 max_block_number_to_read) const;

    BlockInputStreams readFromParts(
        MergeTreeData::DataPartsVector parts,
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        size_t max_block_size,
        unsigned num_streams,
        Int64 max_block_number_to_read) const;

private:
    MergeTreeData & data;

    Logger * log;

    BlockInputStreams spreadMarkRangesAmongStreams(
        RangesInDataParts && parts,
        size_t num_streams,
        const Names & column_names,
        size_t max_block_size,
        bool use_uncompressed_cache,
        const PrewhereInfoPtr & prewhere_info,
        const Names & virt_columns,
        const Settings & settings) const;

    BlockInputStreams spreadMarkRangesAmongStreamsFinal(
        RangesInDataParts && parts,
        const Names & column_names,
        size_t max_block_size,
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
        const MergeTreeData::DataPart::Index & index,
        const KeyCondition & key_condition,
        const Settings & settings) const;
};

}
