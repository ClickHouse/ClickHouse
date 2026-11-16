#pragma once
#include <memory>
#include <Core/ColumnNumbers.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/finalizeChunk.h>

namespace DB
{

struct GroupByModifierTransform : public IAccumulatingTransform
{
    GroupByModifierTransform(SharedHeader header, AggregatingTransformParamsPtr params_, bool use_nulls_);

protected:
    void consume(Chunk chunk) override;

    void mergeConsumed();

    Chunk merge(Chunks && chunks, bool is_input, bool final);

    MutableColumnPtr getColumnWithDefaults(size_t key, size_t n) const;

    /// A single grand-total row over the intermediate header: keys take their default
    /// values (NULLs with group_by_use_nulls) and aggregate states are freshly created
    /// empty states, so finalized aggregates report their empty-input values (count() = 0).
    /// Used when the input is empty and nothing was aggregated.
    Chunk makeGrandTotalRow();

    AggregatingTransformParamsPtr params;

    bool use_nulls;

    ColumnNumbers keys;

    std::unique_ptr<Aggregator> output_aggregator;

    Block intermediate_header;

    Chunks consumed_chunks;
    Chunk current_chunk;

    bool has_input = false;
};

/// Takes blocks after grouping, with non-finalized aggregate functions.
/// Calculates subtotals and grand totals values for a set of columns.
class RollupTransform final : public GroupByModifierTransform
{
public:
    RollupTransform(SharedHeader header, AggregatingTransformParamsPtr params, bool use_nulls_);
    String getName() const override { return "RollupTransform"; }

protected:
    Chunk generate() override;

private:
    const ColumnsMask aggregates_mask;

    size_t last_removed_key = 0;
    size_t set_counter = 0;
    bool generated_grand_total = false;
};

}
