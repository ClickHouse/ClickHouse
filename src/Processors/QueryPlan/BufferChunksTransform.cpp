#include <Processors/QueryPlan/BufferChunksTransform.h>

#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

namespace DB
{

BufferChunksTransform::BufferChunksTransform(
    SharedHeader header_,
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

            const bool virtual_row = isVirtualRow(chunk);
            output.push(std::move(chunk));
            if (virtual_row)
            {
                /// Stop reading until downstream has consumed the virtual-row
                /// marker, otherwise we would pull real chunks past the
                /// part boundary and defeat the LIMIT/read-in-order
                /// optimizations.
                input.setNotNeeded();
                return Status::PortFull;
            }
        }
        else if (input.hasData())
        {
            bool virtual_row;
            auto chunk = pullChunk(virtual_row);
            output.push(std::move(chunk));
            if (virtual_row)
            {
                input.setNotNeeded();
                return Status::PortFull;
            }
        }
    }

    if (input.hasData() && (num_buffered_rows < max_rows_to_buffer || num_buffered_bytes < max_bytes_to_buffer))
    {
        bool virtual_row;
        auto chunk = pullChunk(virtual_row);
        if (virtual_row)
        {
            /// Virtual rows must go to the output immediately.
            /// If the output already has data (from the push above), buffer it
            /// and it will be pushed first on the next prepare() call.
            if (!output.canPush())
            {
                num_buffered_rows += chunk.getNumRows();
                num_buffered_bytes += chunk.bytes();
                chunks.push(std::move(chunk));
                /// The virtual row is now queued; downstream has not yet observed
                /// it, so upstream must not push real chunks past the boundary
                /// before the marker is forwarded.
                input.setNotNeeded();
                return Status::PortFull;
            }
            output.push(std::move(chunk));
            input.setNotNeeded();
            return Status::PortFull;
        }
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

Chunk BufferChunksTransform::pullChunk(bool & virtual_row)
{
    auto chunk = input.pull();
    virtual_row = isVirtualRow(chunk);
    if (!virtual_row)
        num_processed_rows += chunk.getNumRows();

    if (limit && num_processed_rows >= limit)
        input.close();

    return chunk;
}

}
