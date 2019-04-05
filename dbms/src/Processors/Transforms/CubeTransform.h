#pragma once
#include <Processors/IInflatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{

/// Takes blocks after grouping, with non-finalized aggregate functions.
/// Calculates all subsets of columns and aggregates over them.
class CubeTransform : public IInflatingTransform
{
public:
    CubeTransform(Block header, AggregatingTransformParamsPtr params);
    String getName() const override { return "CubeTransform"; }

protected:
    void consume(Chunk chunk) override;

    bool canGenerate() override;

    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    ColumnNumbers keys;

    Chunk consumed_chunk;
    Columns current_columns;
    Columns current_zero_columns;

    UInt64 mask = 0;
};

}
