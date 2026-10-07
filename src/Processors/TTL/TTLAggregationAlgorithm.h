#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>
#include <Interpreters/Aggregator.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Aggregates rows according to 'TTL expr GROUP BY key' description.
/// Aggregation key must be the prefix of the sorting key.
class TTLAggregationAlgorithm final : public ITTLAlgorithm
{
public:
    TTLAggregationAlgorithm(
        const TTLExpressions & ttl_expressions_,
        const TTLDescription & description_,
        const TTLInfo & old_ttl_info_,
        time_t current_time_,
        bool force_,
        const Block & header_,
        const MergeTreeData & storage_,
        const StorageMetadataPtr & metadata_snapshot_);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;

private:
    // Calculate aggregates of aggregate_columns into aggregation_result
    void calculateAggregates(const MutableColumns & aggregate_columns, size_t start_pos, size_t length);

    /// Finalize aggregation_result into result_columns
    void finalizeAggregates(MutableColumns & result_columns);

    /// An aggregated row is emitted at the position of its group in the sorted stream, but `SET` may assign
    /// arbitrary values to the columns the sorting key is calculated from, so the row may be less than the
    /// previously emitted one. In such a row, the columns the sorting key is calculated from are replaced with
    /// the values of the previous row, so its sorting key becomes equal to the sorting key of the previous row
    /// and the output stays sorted. The sorting key columns of the header (they are used for the primary index)
    /// are recalculated from the data, because after `SET` they may hold the values calculated before it.
    void restoreSortOrder(Block & block);

    /// Calculates the sorting key columns from the columns of the block.
    Columns calculateSortingKey(const Block & block) const;

    const Block header;
    /// Calculates the sorting key from the columns of the header. Empty if the table has no sorting key.
    ExpressionActionsPtr sorting_key_expression;
    /// The columns of the header the sorting key is calculated from.
    Names sorting_key_required_columns;
    /// The full sorting key. The header may lack some of its columns: a mutation calculates only the primary key.
    SortDescription sort_description;
    /// The sorting key of the last emitted row and the columns it was calculated from, to check the order across blocks.
    Columns last_row_sort_key;
    Columns last_row_required_columns;
    std::unique_ptr<Aggregator> aggregator;
    Row current_key_value;
    AggregatedDataVariants aggregation_result;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns columns_for_aggregator;
    bool no_more_keys = false;
};

}
