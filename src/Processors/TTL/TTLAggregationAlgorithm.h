#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>
#include <Interpreters/Aggregator.h>
#include <Storages/MergeTree/MergeTreeData.h>

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
        const MergeTreeData & storage_);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;

private:
    // Calculate aggregates of aggregate_columns into aggregation_result
    void calculateAggregates(const MutableColumns & aggregate_columns, size_t start_pos, size_t length);

    /// Finalize aggregation_result into result_columns
    void finalizeAggregates(MutableColumns & result_columns);

    const Block header;
    std::unique_ptr<Aggregator> aggregator;
    Row current_key_value;
    AggregatedDataVariants aggregation_result;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns columns_for_aggregator;
    bool no_more_keys = false;
};

}
