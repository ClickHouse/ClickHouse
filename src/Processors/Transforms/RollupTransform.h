#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Takes blocks after grouping, with non-finalized aggregate functions.
/// Calculates subtotals and grand totals values for a set of columns.
class RollupTransform : public IAccumulatingTransform
{
public:
    RollupTransform(Block header, AggregatingTransformParamsPtr params);
    String getName() const override { return "RollupTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    ColumnNumbers keys;
    Chunks consumed_chunks;
    Chunk rollup_chunk;
    size_t last_removed_key = 0;

    Chunk merge(Chunks && chunks, bool final);
};

}
