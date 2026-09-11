#pragma once
#include <limits>
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
class RollupTransform final : public GroupByModifierTransform
{
public:
    /// `key_positions_` maps each element of the GROUP BY list, in order and keeping repetitions,
    /// onto its index in the deduplicated key list. Empty means the trivial 0..keys-1 mapping.
    RollupTransform(SharedHeader header, AggregatingTransformParamsPtr params, bool use_nulls_,
                    const std::vector<size_t> & key_positions_ = {});
    String getName() const override { return "RollupTransform"; }

protected:
    Chunk generate() override;

private:
    const ColumnsMask aggregates_mask;

    /// Number of elements written in the GROUP BY list, which is what ROLLUP takes the prefixes of.
    size_t num_group_by_elements = 0;
    /// For each GROUP BY position, the key to drop when the prefix shrinks past it, or `no_key` when
    /// the key written there also occurs earlier and so is still held by that earlier position.
    static constexpr size_t no_key = std::numeric_limits<size_t>::max();
    std::vector<size_t> key_dropped_at_position;

    size_t last_removed_position = 0;
    size_t set_counter = 0;
};

}
