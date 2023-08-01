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
    GroupByModifierTransform(Block header, AggregatingTransformParamsPtr params_, bool use_nulls_);

protected:
    void consume(Chunk chunk) override;

    void mergeConsumed();

    Chunk merge(Chunks && chunks, bool is_input, bool final);

    MutableColumnPtr getColumnWithDefaults(size_t key, size_t n) const;

    AggregatingTransformParamsPtr params;

    bool use_nulls;

    ColumnNumbers keys;

    std::unique_ptr<Aggregator> output_aggregator;

    Block intermediate_header;

    Chunks consumed_chunks;
    Chunk current_chunk;
};

/// Takes blocks after grouping, with non-finalized aggregate functions.
/// Calculates subtotals and grand totals values for a set of columns.
class RollupTransform : public GroupByModifierTransform
{
public:
    RollupTransform(Block header, AggregatingTransformParamsPtr params, bool use_nulls_);
    String getName() const override { return "RollupTransform"; }

protected:
    Chunk generate() override;

private:
    const ColumnsMask aggregates_mask;

    size_t last_removed_key = 0;
    size_t set_counter = 0;
};

}
