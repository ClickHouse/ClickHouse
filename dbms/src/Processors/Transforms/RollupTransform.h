#pragma once
#include <Processors/IInflatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Takes blocks after grouping, with non-finalized aggregate functions.
/// Calculates subtotals and grand totals values for a set of columns.
class RollupTransform : public IInflatingTransform
{
public:
    RollupTransform(Block header, AggregatingTransformParamsPtr params);
    String getName() const override { return "RollupTransform"; }

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    ColumnNumbers keys;
    Chunk consumed_chunk;
    size_t last_removed_key = 0;
};

}
