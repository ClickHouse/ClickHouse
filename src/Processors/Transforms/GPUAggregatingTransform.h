#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUAccumulator.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IAccumulatingTransform.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

namespace DB
{

class GPUAggregatingTransform final : public IAccumulatingTransform
{
public:
    GPUAggregatingTransform(
        const SharedHeader & input_header_, const SharedHeader & output_header_, const Aggregator::Params & params, size_t batch_bytes);

    String getName() const override { return "GPUAggregatingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    Chunk generateGroups(const Block & header);

private:
    VectorWithMemoryTracking<size_t> key_positions;
    /// Without keys: of the argument of each aggregate. With keys: of each column some aggregates are of, once.
    VectorWithMemoryTracking<size_t> argument_positions;
    size_t num_aggregates = 0;

    std::optional<GPU::GPUAccumulator> accumulator;
    std::optional<GPU::GroupByGPUAccumulator> group_by_accumulator;

    const bool empty_result_for_empty_set;

    size_t total_rows = 0;
    bool generated = false;
};

}

#endif
