#pragma once
#include <Processors/IProcessor.h>
#include <queue>

namespace DB
{

/// Transform that buffers chunks from the input
/// up to the certain limit  and pushes chunks to
/// the output whenever it is ready. It can be used
/// to increase parallelism of execution, for example
/// when it is adeded before MergingSortedTransform.
class BufferChunksTransform : public IProcessor
{
public:
    /// OR condition is used for the limits on rows and bytes.
    BufferChunksTransform(
        const Block & header_,
        size_t max_rows_to_buffer_,
        size_t max_bytes_to_buffer_,
        size_t limit_);

    Status prepare() override;
    String getName() const override { return "BufferChunks"; }

private:
    Chunk pullChunk();

    InputPort & input;
    OutputPort & output;

    size_t max_rows_to_buffer;
    size_t max_bytes_to_buffer;
    size_t limit;

    std::queue<Chunk> chunks;
    size_t num_buffered_rows = 0;
    size_t num_buffered_bytes = 0;
    size_t num_processed_rows = 0;
};

}
