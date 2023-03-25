#pragma once

#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Implementation for OFFSET N (without limit)
/// This processor support multiple inputs and outputs (the same number).
/// Each pair of input and output port works independently.
class OffsetTransform final : public IProcessor
{
private:
    const UInt64 offset;
    UInt64 rows_read = 0; /// including the last read block

    RowsBeforeLimitCounterPtr rows_before_limit_at_least;

    /// State of port's pair.
    /// Chunks from different port pairs are not mixed for better cache locality.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_finished = false;

        /// When offset is negative,
        /// a input port is finished does not mean its output port need to be finished.
        bool is_input_port_finished = false;
        bool is_output_port_finished = false;
    };

    std::vector<PortsData> ports_data;
    size_t num_finished_port_pairs = 0;
    size_t num_finished_input_port = 0; /// used when offset is negative
    size_t num_finished_output_port = 0; /// used when offset is negative

    struct QueueElement
    {
        Chunk chunk;
        size_t port;
    };

    const bool is_negative;
    std::list<QueueElement> queue; /// used when offset is negative, storing at least offset rows
    UInt64 rows_in_queue = 0;

    QueueElement popAndCutIfNeeded();
    void queuePushBack(QueueElement & element);
    QueueElement queuePopFront();
    void skipChunksForFinishedOutputPorts();
    Status loopPop();

    Status prepareNonNegative(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/);
    Status prepareNegative(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/);

public:
    OffsetTransform(const Block & header_, UInt64 offset_, size_t num_streams = 1, bool is_negative_ = false);

    String getName() const override { return "Offset"; }

    Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) override;
    Status prepare() override; /// Compatibility for TreeExecutor.
    Status preparePairNonNegative(PortsData & data);
    void preparePairNegative(size_t pos);
    void splitChunk(PortsData & data) const;

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeLimitCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
};

}
