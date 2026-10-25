#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUAccumulator.h>
#include <Interpreters/Aggregator.h>
#include <Processors/IAccumulatingTransform.h>

#include <optional>

namespace DB
{

class GPUAggregatingTransform final : public IAccumulatingTransform
{
public:
    GPUAggregatingTransform(
        const SharedHeader & input_header_, const SharedHeader & output_header_, const Aggregator::Params & params, size_t batch_bytes);

    String getName() const override { return "GPUAggregatingTransform"; }

    /// The columns on the device are aggregated there, see `DeviceChunkInfo`.
    bool acceptsDeviceChunks() const override { return true; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    Chunk generateGroups(const Block & header);

private:
    std::vector<size_t> key_positions;
    /// Without keys: of the argument of each aggregate. With keys: of each column some aggregates are of, once.
    std::vector<size_t> argument_positions;
    size_t num_aggregates = 0;

    std::vector<GPU::GPUAccumulator> accumulators;
    std::optional<GPU::GroupByGPUAccumulator> group_by_accumulator;

    const bool empty_result_for_empty_set;

    size_t total_rows = 0;
    bool generated = false;

    /// The groups, given out in chunks of `groups_per_chunk` rows, so that the steps above take them in parallel.
    static constexpr size_t groups_per_chunk = 1UL << 18;
    Columns groups;
    size_t groups_rows = 0;
    size_t groups_offset = 0;
    Chunk nextGroupsChunk();
};

}

#endif
