#pragma once
#include <memory>
#include <Core/ColumnNumbers.h>
#include <Common/DequeWithMemoryTracking.h>
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
///
/// Each level merges the states of the previous one by one key less. A level with many rows is merged by
/// `max_threads` threads: its rows are split by the hash of the level's keys, so equal keys land in the same
/// part, and the parts are merged independently. A level is then a list of chunks, one per part.
class RollupTransform final : public GroupByModifierTransform
{
public:
    RollupTransform(SharedHeader header, AggregatingTransformParamsPtr params, bool use_nulls_);
    String getName() const override { return "RollupTransform"; }

protected:
    Chunk generate() override;

private:
    /// Merges `chunks` by the first `num_keys` keys (the other keys hold one value in all rows). `is_input` is as
    /// for `merge`.
    Chunks mergeLevel(Chunks && chunks, size_t num_keys, bool is_input);

    const ColumnsMask aggregates_mask;

    size_t last_removed_key = 0;
    size_t set_counter = 0;

    /// The states of the level to output next, and whether there is one.
    Chunks current_level;
    bool has_current_level = false;
    /// Finalized chunks of the level being output.
    DequeWithMemoryTracking<Chunk> ready_chunks;
};

}
