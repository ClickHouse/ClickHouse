#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUAccumulator.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IAccumulatingTransform.h>

#include <deque>
#include <optional>

namespace DB
{

class GPUAggregatingTransform final : public IAccumulatingTransform
{
public:
    /// With `input_grouped_`, the input is one row per group already, and the transform only
    /// puts its columns in the order and under the names of the output header.
    GPUAggregatingTransform(
        const SharedHeader & input_header_,
        const SharedHeader & output_header_,
        const Aggregator::Params & params,
        size_t batch_bytes,
        bool input_grouped_ = false);

    String getName() const override { return "GPUAggregatingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    Chunk generateGroups(const Block & header);

private:
    std::vector<size_t> key_positions;
    std::vector<size_t> argument_positions;

    std::vector<GPU::GPUAccumulator> accumulators;
    std::optional<GPU::GroupByGPUAccumulator> group_by_accumulator;

    const bool empty_result_for_empty_set;
    const bool input_grouped;

    std::deque<Chunk> grouped_chunks;

    size_t total_rows = 0;
    bool generated = false;
};

}

#endif
