#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <queue>

namespace DB
{
class Block;

/// Transform that buffers chunks from the input
/// up to the certain limit  and pushes chunks to
/// the output whenever it is ready. It can be used
/// to increase parallelism of execution, for example
/// when it is adeded before MergingSortedTransform.
class BufferChunksTransform final : public IProcessor
{
public:
    /// OR condition is used for the limits on rows and bytes.
    BufferChunksTransform(
        SharedHeader header_,
        size_t max_rows_to_buffer_,
        size_t max_bytes_to_buffer_,
        size_t limit_);

    Status prepare() override;
    String getName() const override { return "BufferChunks"; }

private:
    Chunk pullChunk(bool & virtual_row);

    InputPort & input;
    OutputPort & output;

    size_t max_rows_to_buffer;
    size_t max_bytes_to_buffer;
    size_t limit;

    std::queue<Chunk> chunks;
    size_t num_buffered_rows = 0;
    size_t num_buffered_bytes = 0;
    size_t num_processed_rows = 0;

    /// After a virtual row was delivered downstream, the merge may defer this source
    /// (leave the port NotNeeded) until the merge reaches the key from the virtual row.
    /// While that is the case, do not read ahead into the buffer, otherwise buffering
    /// would defeat the deferral and every source would read speculatively regardless
    /// of `virtual_row_prefetch_window`. Without this flag the invariant only holds by
    /// an executor subtlety: the merge consumes the virtual row with `pull(true)`, which
    /// skips `updateVersion`, so nothing re-schedules this transform and the final
    /// `input.setNeeded()` of `prepare` is never reached. This flag makes the invariant
    /// explicit instead of relying on the wake-up pattern of the downstream processor.
    bool wait_for_demand_after_virtual_row = false;
};

}
