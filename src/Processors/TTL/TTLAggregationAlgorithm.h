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
        const MergeTreeData & storage_,
        bool input_sorted_by_group_by_keys_ = true);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;

private:
    // Calculate aggregates of aggregate_columns into aggregation_result
    void calculateAggregates(const MutableColumns & aggregate_columns, size_t start_pos, size_t length);

    /// Finalize aggregation_result into result_columns
    void finalizeAggregates(MutableColumns & result_columns);

    /// Apply the SET expressions to one finalized aggregated block and append it to result_columns
    void appendAggregatedBlock(Block agg_block, MutableColumns & result_columns);

    /// Streaming path for input already ordered by group_by_keys: flush each key run as soon as a
    /// new key is observed.
    void executeSorted(Block & block, MutableColumns & result_columns, bool & some_rows_were_aggregated);
    /// Path for input that may NOT be ordered by group_by_keys (an earlier GROUP BY TTL's SET can
    /// rewrite a key here): accumulate every expired row into the aggregation state across all
    /// blocks and finalize once at end of stream, so non-contiguous runs of the same key merge into
    /// one group instead of fragmenting.
    void executeUnsorted(Block & block, MutableColumns & result_columns, bool & some_rows_were_aggregated);

    const Block header;
    std::unique_ptr<Aggregator> aggregator;
    Row current_key_value;
    AggregatedDataVariants aggregation_result;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns columns_for_aggregator;
    bool no_more_keys = false;
    /// Whether the input stream is guaranteed to be ordered by this TTL's group_by_keys. False when
    /// an earlier GROUP BY TTL's SET rewrites one of these keys; see executeUnsorted.
    const bool input_sorted_by_group_by_keys;
};

}
