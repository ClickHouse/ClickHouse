#pragma once
#include <Processors/IInflatingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/finalizeChunk.h>


namespace DB
{

/// Takes blocks after grouping, with non-finalized aggregate functions.
/// Calculates all subsets of columns and aggregates over them.
class CubeTransform final : public GroupByModifierTransform
{
public:
    /// `key_positions_` maps each element of the GROUP BY list, in order and keeping repetitions,
    /// onto its index in the deduplicated key list. Empty means the trivial 0..keys-1 mapping.
    CubeTransform(SharedHeader header, AggregatingTransformParamsPtr params, bool use_nulls_,
                  const std::vector<size_t> & key_positions_ = {});
    String getName() const override { return "CubeTransform"; }

protected:
    Chunk generate() override;

private:
    /// The `__grouping_set` number for the subset described by `position_mask`.
    UInt64 groupingSetForMask(UInt64 position_mask) const;

private:
    const ColumnsMask aggregates_mask;

    Columns current_columns;
    Columns current_zero_columns;

    /// Number of elements written in the GROUP BY list, which is what CUBE takes the power set of.
    /// Equal to the number of keys unless an expression was repeated.
    size_t num_group_by_elements = 0;
    /// For each deduplicated key, the set of GROUP BY positions it was written at. A key survives a
    /// subset iff *any* of its positions is in it, so a repeated key is kept until every one of its
    /// positions has been dropped.
    std::vector<UInt64> position_masks;

    UInt64 mask = 0;
};

}
