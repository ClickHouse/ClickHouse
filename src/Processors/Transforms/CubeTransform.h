#pragma once
#include <Processors/IInflatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/finalizeChunk.h>


namespace DB
{

/// Takes blocks after grouping, with non-finalized aggregate functions.
/// Calculates all subsets of columns and aggregates over them.
class CubeTransform : public GroupByModifierTransform
{
public:
    CubeTransform(Block header, AggregatingTransformParamsPtr params, bool use_nulls_);
    String getName() const override { return "CubeTransform"; }

protected:
    Chunk generate() override;

private:
    const ColumnsMask aggregates_mask;

    Columns current_columns;
    Columns current_zero_columns;

    UInt64 mask = 0;
    UInt64 grouping_set = 0;
};

}
