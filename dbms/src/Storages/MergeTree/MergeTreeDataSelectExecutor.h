#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

class PKCondition;


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
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned threads,
        size_t * inout_part_index,    /// If not nullptr, from this counter values are taken for the virtual column _part_index.
        Int64 max_block_number_to_read) const;

private:
    MergeTreeData & data;

    Logger * log;

    BlockInputStreams spreadMarkRangesAmongThreads(
        RangesInDataParts parts,
        size_t threads,
        const Names & column_names,
        size_t max_block_size,
        bool use_uncompressed_cache,
        ExpressionActionsPtr prewhere_actions,
        const String & prewhere_column,
        const Names & virt_columns,
        const Settings & settings) const;

    BlockInputStreams spreadMarkRangesAmongThreadsFinal(
        RangesInDataParts parts,
        size_t threads,
        const Names & column_names,
        size_t max_block_size,
        bool use_uncompressed_cache,
        ExpressionActionsPtr prewhere_actions,
        const String & prewhere_column,
        const Names & virt_columns,
        const Settings & settings,
        const Context & context) const;

    /// Get the approximate value (bottom estimate - only by full marks) of the number of rows falling under the index.
    size_t getApproximateTotalRowsToRead(
        const MergeTreeData::DataPartsVector & parts,
        const PKCondition & key_condition,
        const Settings & settings) const;

    /// Create the expression "Sign == 1".
    void createPositiveSignCondition(
        ExpressionActionsPtr & out_expression,
        String & out_column,
        const Context & context) const;

    MarkRanges markRangesFromPKRange(
        const MergeTreeData::DataPart::Index & index,
        const PKCondition & key_condition,
        const Settings & settings) const;
};

}
