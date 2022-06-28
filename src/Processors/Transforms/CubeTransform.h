#pragma once
#include <Processors/IInflatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/finalizeChunk.h>


namespace DB
{

/// Takes blocks after grouping, with non-finalized aggregate functions.
/// Calculates all subsets of columns and aggregates over them.
class CubeTransform : public IAccumulatingTransform
{
public:
    CubeTransform(Block header, AggregatingTransformParamsPtr params);
    String getName() const override { return "CubeTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    ColumnNumbers keys;
    const ColumnsMask aggregates_mask;

    Chunks consumed_chunks;
    Chunk cube_chunk;
    Columns current_columns;
    Columns current_zero_columns;

    UInt64 mask = 0;
    UInt64 grouping_set = 0;

    Chunk merge(Chunks && chunks, bool final);
};

}
