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
    /// `key_positions_`: index in the deduplicated key list of each `GROUP BY` element as written; empty if no key repeats.
    CubeTransform(SharedHeader header, AggregatingTransformParamsPtr params, bool use_nulls_,
                  const std::vector<size_t> & key_positions_ = {});
    String getName() const override { return "CubeTransform"; }

protected:
    Chunk generate() override;

private:
    /// The `__grouping_set` number for the subset described by `position_mask`.
    UInt64 groupingSetForMask(UInt64 position_mask) const;

    const ColumnsMask aggregates_mask;

    Columns current_columns;
    Columns current_zero_columns;

    /// `CUBE` takes the power set of the list as written, not of the deduplicated keys.
    size_t num_group_by_elements = 0;
    /// Per key, the positions it was written at; a key stays in a subset while any of them does.
    std::vector<UInt64> position_masks;

    UInt64 mask = 0;
};

}
