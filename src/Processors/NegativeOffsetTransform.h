#pragma once

#include <queue>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeStepCounter.h>

namespace DB
{

/// Implementation for OFFSET -N (without limit) (drop last N)
/// This processor support multiple inputs and outputs (the same number).
/// Each pair of input and output port works independently.

/// Streams by keeping a tail buffer of size N (in rows) across all input ports.
class NegativeOffsetTransform final : public IProcessor
{
private:
    UInt64 offset;

    /// Total rows currently queued across all inputs.
    UInt64 queued_row_count = 0;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// State of port's pair.
    struct PortsData
    {
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_input_port_finished = false;
    };

    UInt64 num_input_ports_finished = 0;

    std::vector<PortsData> ports_data;

    /// `Pull` stage: it ends when all input ports are closed.
    /// `Push` stage: it starts immediately after the `Pull` stage and it ends
    ///               when all queued full/partial chunks are pushed to output ports excluding the offset.
    enum class Stage : uint8_t
    {
        Pull = 0,
        Push
    };

    Stage stage = Stage::Pull;

    struct ChunkWithPort
    {
        OutputPort * output_port = nullptr;
        Chunk chunk;
    };

    /// Stores the pending chunks which are not yet confirmed whether they are
    /// full outside the offset or not. Once we can be sure that a chunk is fully
    /// outside the offset, it is pushed to the output port and popped from the queue.
    std::queue<ChunkWithPort> queue;

public:
    NegativeOffsetTransform(const Block & header_, UInt64 offset_, size_t num_streams = 1);

    String getName() const override { return "NegativeOffset"; }

    Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) override;
    Status prepare() override; /// Compatibility for TreeExecutor.

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }

private:
    /// Process a single input port, populates `queue`.
    Status advancePort(PortsData & data);

    /// Does not do anything if the front chunk is not fully outside the offset.
    Status tryPushWholeFrontChunk();

    /// This should only be called when we know that the front chunk is NOT
    /// fully outside the offset.
    Status tryPushRemainingChunkPrefix();
};

}
