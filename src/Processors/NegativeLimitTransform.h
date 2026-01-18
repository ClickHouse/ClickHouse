#pragma once

#include <queue>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeStepCounter.h>

namespace DB
{

/// Implementation for LIMIT -N OFFSET -M (drops last M rows, then gives last N rows)
/// This processor support multiple inputs and outputs (the same number).
/// Each pair of input and output port works independently.
/// The reason to have multiple ports is to be able to stop all sources when limit is reached, in a query like:
///     SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT -1
///
/// For now, with_ties is not supported.
class NegativeLimitTransform final : public IProcessor
{
private:
    UInt64 limit;
    UInt64 offset;

    /// Total rows currently queued across all inputs.
    UInt64 queued_row_count = 0;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// State of port's pair.
    /// Chunks from different port pairs are not mixed for better cache locality.
    struct PortsData
    {
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_input_port_finished = false;

        /// This flag is used to avoid counting rows multiple times before applying a limit
        /// condition, which can happen through certain input ports like PartialSortingTransform and
        /// RemoteSource.
        bool input_port_has_counter = false;
    };

    UInt64 num_input_ports_finished = 0;

    std::vector<PortsData> ports_data;

    /// `Pull` stage: it ends when all input ports are closed.
    /// `Push` stage: it starts immediately after the `Pull` stage and it ends
    ///               when all queued full/partial chunks within limit are pushed
    ///               to output ports excluding the offset.
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
    /// full outside the limit + offset or not. Once we can be sure that a chunk is fully
    /// outside the limit + offset, it is removed from the queue.
    std::queue<ChunkWithPort> queue;

public:
    NegativeLimitTransform(SharedHeader header_, UInt64 limit_, UInt64 offset_, size_t num_streams = 1);

    String getName() const override { return "NegativeLimit"; }

    Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) override;
    Status prepare() override; /// Compatibility for TreeExecutor.

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
    void setInputPortHasCounter(size_t pos) { ports_data[pos].input_port_has_counter = true; }

private:
    /// Process a single input port, populates `queue`.
    Status advancePort(PortsData & data);

    /// Tries to push the Suffix part of the front chunk of the queue that is within LIMIT
    /// and Prefix part might be outside LIMIT + OFFSET
    Status tryPushChunkSuffixWithinLimit();

    /// Tries to push the front chunk of the queue that is completely within LIMIT
    Status tryPushWholeFrontChunk();

    /// Tries to push the Prefix part of the front chunk of the queue that is within LIMIT
    /// and Suffix part might be inside OFFSET
    Status tryPushChunkPrefixWithinLimit();
};

}
