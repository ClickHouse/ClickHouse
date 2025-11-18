#pragma once

#include <deque>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <base/types.h>

namespace DB
{

/// Implementation for OFFSET N (without limit)
//  where N is a fraction in (0, 1) non-inclusive representing a percentage.
//
/// This processor supports multiple inputs and outputs (the same number).
/// Each pair of input and output port works independently.
///
/// Processor workflow:
/// while input:
///     1. read and cache input chunk
///     2. drop from cache chunks that 
///        we became 100% sure they will be offseted.
/// 2. count total rows count in input
/// 3. calculate integral offset = (fractional_offset * rows_cnt) - evicted_rows_cnt
/// 4. apply normal offset logic on remaining cached chunks.
class FractionalOffsetTransform final : public IProcessor
{
private:
    Float64 fractional_offset;
    UInt64 offset = 0;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// State of port's pair.
    /// Chunks from different port pairs are not mixed for better cache locality.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_input_port_finished = false;
    };

    std::vector<PortsData> ports_data;
    size_t num_finished_input_ports = 0;

    UInt64 rows_cnt = 0;
    UInt64 evicted_rows_cnt = 0;
    struct CacheEntity
    {
        OutputPort * output_port = nullptr;
        Chunk chunk;
    };
    std::deque<CacheEntity> chunks_cache;

public:
    FractionalOffsetTransform(const Block & header_, Float64 fractional_offset_, size_t num_streams = 1);

    String getName() const override { return "FractionalOffset"; }

    Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) override;
    Status prepare() override; /// Compatibility for TreeExecutor.
    Status pullData(PortsData & data);
    Status pushData();
    void splitChunk(Chunk & current_chunk) const;

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
};

}
