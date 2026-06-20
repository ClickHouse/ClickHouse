#pragma once

#include <deque>
#include <functional>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{

/// Scatters input rows to N output ports using a caller-supplied selector.
/// For every input chunk the selector decides the destination port of each row, and every column
/// is physically split with IColumn::scatter so each output chunk holds only the rows of its port.
///
/// Output ports can only accept one chunk at a time (canPush/push). But one input chunk produces up
/// to N output chunks (one per port), and downstream consume them at different rates. Without
/// queueing, we would have to wait until all N outputs are ready before splitting an input chunk —
/// one slow consumer would stall all others.
///
/// So each output port has a FIFO queue. When a port is busy, its chunk waits in the queue and gets
/// pushed on the next prepare()/work() cycle. This allows other ports to continue processing without
/// waiting for the slowest one.
class BufferedScatterTransform : public IProcessor
{
public:
    /// Builds the per-row destination port for an input chunk. It must return a Selector of size
    /// num_rows with every value in [0, num_outputs); columns are the input chunk's columns.
    using SelectorBuilder = std::function<IColumn::Selector(const Columns & columns)>;

    BufferedScatterTransform(SharedHeader header, size_t num_outputs_, SelectorBuilder selector_builder_);

    String getName() const override { return "BufferedScatterTransform"; }

    Status prepare() override;
    void work() override;

private:
    void generateOutputChunks();

    /// Once any queue hits this length the transform stops pulling new input until
    /// the slow consumer drains it. Otherwise, we can have very high memory usage.
    static constexpr size_t MAX_QUEUE_LENGTH = 10;

    size_t num_outputs;
    SelectorBuilder selector_builder;

    /// Input chunk that was pulled in prepare() and will be split in work().
    bool has_pending_input_chunk = false;
    Chunk pending_input_chunk;

    /// Per-output FIFO of chunks waiting to be pushed downstream. Bounded at MAX_QUEUE_LENGTH.
    std::vector<std::deque<Chunk>> output_queues;

    /// Reused across input chunks to skip per-chunk reallocation.
    std::vector<MutableColumns> output_columns;
};

}
