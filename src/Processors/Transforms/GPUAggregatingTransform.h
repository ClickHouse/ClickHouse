#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUAggregation.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IAccumulatingTransform.h>

#include <optional>

namespace DB
{

/// Sums the argument column of every chunk it is given on the device, and produces the rows the
/// aggregation returns once its input is done: one row without `GROUP BY`, and one per group with
/// it. See `GPUAggregatingStep`.
class GPUAggregatingTransform final : public IAccumulatingTransform
{
public:
    GPUAggregatingTransform(
        const SharedHeader & input_header_,
        const SharedHeader & output_header_,
        const Aggregator::Params & params,
        size_t batch_bytes);

    String getName() const override { return "GPUAggregatingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    /// The keyed half of `generate`, kept apart because it has nothing in common with the keyless
    /// one beyond producing a chunk.
    Chunk generateGroups(const Block & header);

private:
    /// Where in the input the columns are that this reads: the keys in `params.keys` order and the
    /// summed argument of each aggregate in `params.aggregates` order - which is also the order the
    /// output header puts them in, keys first. `key_positions` is empty exactly when the
    /// aggregation has no `GROUP BY`.
    std::vector<size_t> key_positions;
    std::vector<size_t> argument_positions;

    /// One or the other, never both: the keyless aggregation holds one accumulator per aggregate
    /// function, and the keyed one holds a single accumulator that carries all of them, because its
    /// partial result is one table of groups with a column per `sum` rather than a scalar each.
    std::vector<GPU::SumAccumulator> accumulators;
    std::optional<GPU::GroupBySumAccumulator> group_by_accumulator;

    const bool empty_result_for_empty_set;

    size_t total_rows = 0;
    bool generated = false;
};

}

#endif
