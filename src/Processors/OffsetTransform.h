#pragma once

#include <Core/SortDescription.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeStepCounter.h>

namespace DB
{

/// Implementation for OFFSET N (without limit)
/// This processor support multiple inputs and outputs (the same number).
/// Each pair of input and output port works independently.
class OffsetTransform final : public IProcessor
{
private:
    UInt64 offset;
    UInt64 rows_read = 0; /// including the last read block

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// State of port's pair.
    /// Chunks from different port pairs are not mixed for better cache locality.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_finished = false;
    };

    std::vector<PortsData> ports_data;
    size_t num_finished_port_pairs = 0;

public:
    OffsetTransform(const Block & header_, UInt64 offset_, size_t num_streams = 1);

    String getName() const override { return "Offset"; }

    Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) override;
    Status prepare() override; /// Compatibility for TreeExecutor.
    Status preparePair(PortsData & data);
    void splitChunk(PortsData & data) const;

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
};

}
