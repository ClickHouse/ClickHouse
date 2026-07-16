#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IAccumulatingTransform.h>

namespace DB
{

/// Fused GROUP BY ... ORDER BY aggregate LIMIT K transform for sorted input (Mode 1).
///
/// Aggregation is delegated to the standard Aggregator (fast type-dispatched / LowCardinality
/// hash methods, arena-managed states). Input is sorted by the ORDER BY aggregate's argument, so
/// each group's aggregate is decided by its first row; the transform stops once K distinct groups
/// exist (no unseen group can outrank them), then orders the groups and keeps the top K.
class TopNSortedAggregatingTransform : public IAccumulatingTransform
{
public:
    TopNSortedAggregatingTransform(
        const Block & input_header_,
        const Block & output_header_,
        Aggregator::Params params_,
        const SortDescription & sort_description_,
        size_t limit_);

    String getName() const override { return "TopNSortedAggregating"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    SortDescription sort_description;
    size_t limit;
    /// Index of the ORDER BY aggregate column in the output header.
    size_t order_column_index = 0;

    /// Declared before `variants` so it outlives it: ~AggregatedDataVariants
    /// destroys aggregate states through its `aggregator` back-pointer.
    Aggregator aggregator;
    AggregatedDataVariants variants;

    /// Scratch buffers reused across blocks by Aggregator::executeOnBlock.
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    bool no_more_keys = false;

    bool has_data = false;
    bool generated = false;
};

}
