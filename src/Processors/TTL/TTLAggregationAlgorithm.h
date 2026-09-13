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
    /// arbitrary values to the sorting key columns, so the row may be less than the previously emitted one.
    /// The sorting key of such a row is replaced with the sorting key of the previous row to keep the output sorted.
    void restoreSortOrder(Block & block);

    const Block header;
    /// The sorting key columns that are present in the header.
    SortDescription sort_description;
    /// The sorting key of the last emitted row, to check the order across blocks.
    Columns last_row_sort_key;
    std::unique_ptr<Aggregator> aggregator;
    Row current_key_value;
    AggregatedDataVariants aggregation_result;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns columns_for_aggregator;
    bool no_more_keys = false;
};

}
