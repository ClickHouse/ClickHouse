#include <Processors/QueryPlan/BufferChunksTransform.h>

namespace DB
{

BufferChunksTransform::BufferChunksTransform(
    const Block & header_,
    size_t max_rows_to_buffer_,
    size_t max_bytes_to_buffer_,
    size_t limit_)
    : IProcessor({header_}, {header_})
    , input(inputs.front())
    , output(outputs.front())
    , max_rows_to_buffer(max_rows_to_buffer_)
    , max_bytes_to_buffer(max_bytes_to_buffer_)
    , limit(limit_)
{
}

IProcessor::Status BufferChunksTransform::prepare()
{
    if (output.isFinished())
    {
        chunks = {};
        input.close();
        return Status::Finished;
    }

    if (input.isFinished() && chunks.empty())
    {
        output.finish();
        return Status::Finished;
    }

    if (output.canPush())
    {
        input.setNeeded();

        if (!chunks.empty())
        {
            auto chunk = std::move(chunks.front());
            chunks.pop();

            num_buffered_rows -= chunk.getNumRows();
            num_buffered_bytes -= chunk.bytes();

            output.push(std::move(chunk));
        }
        else if (input.hasData())
        {
            auto chunk = pullChunk();
            output.push(std::move(chunk));
        }
    }

    if (input.hasData() && (num_buffered_rows < max_rows_to_buffer || num_buffered_bytes < max_bytes_to_buffer))
    {
        auto chunk = pullChunk();
        num_buffered_rows += chunk.getNumRows();
        num_buffered_bytes += chunk.bytes();
        chunks.push(std::move(chunk));
    }

    if (num_buffered_rows >= max_rows_to_buffer && num_buffered_bytes >= max_bytes_to_buffer)
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    input.setNeeded();
    return Status::NeedData;
}

Chunk BufferChunksTransform::pullChunk()
{
    auto chunk = input.pull();
    num_processed_rows += chunk.getNumRows();

    if (limit && num_processed_rows >= limit)
        input.close();

    return chunk;
}

}
