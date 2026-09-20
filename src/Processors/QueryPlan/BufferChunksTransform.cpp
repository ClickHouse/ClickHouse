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

    /// Do not read ahead while the downstream merge may have deferred this source
    /// after seeing its virtual row. Resume only when data is actually demanded —
    /// either right away (the source is not deferred) or when the merge releases
    /// the deferred source (see `IMergingTransformBase` and `topUpPrefetch`).
    if (wait_for_demand_after_virtual_row)
    {
        if (!output.canPush())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        wait_for_demand_after_virtual_row = false;
    }

    if (output.canPush())
    {
        input.setNeeded();

        if (!chunks.empty())
        {
            if (pushBufferedChunk())
                return Status::PortFull;
        }
        else if (input.hasData())
        {
            bool virtual_row = false;
            auto chunk = pullChunk(virtual_row);
            output.push(std::move(chunk));
            if (virtual_row)
            {
                wait_for_demand_after_virtual_row = true;
                input.setNotNeeded();
                return Status::PortFull;
            }
        }
    }

    if (input.hasData() && (num_buffered_rows < max_rows_to_buffer || num_buffered_bytes < max_bytes_to_buffer))
    {
        bool virtual_row = false;
        auto chunk = pullChunk(virtual_row);
        if (virtual_row)
        {
            /// A virtual row announces the boundary of everything that follows it in this
            /// stream, so it must keep its place: it may never overtake chunks already
            /// buffered ahead of it, or the merge sees a boundary that the stream's own next
            /// rows violate (`Virtual row boundary violated in MergingSortedAlgorithm`).
            /// `output.canPush()` is not a safe proxy for "nothing is buffered": the
            /// downstream runs concurrently with this `prepare()` and can drain the output
            /// port right after the push above, while `chunks` still holds earlier data.
            /// So always queue the marker, and let the drain below emit it in order.
            num_buffered_rows += chunk.getNumRows();
            num_buffered_bytes += chunk.bytes();
            chunks.push(std::move(chunk));

            /// Downstream has not observed the marker yet, so upstream must not push real
            /// chunks past the boundary before it is forwarded.
            input.setNotNeeded();

            /// Emit the head of the queue right away when the output has room. This keeps the
            /// marker as prompt as a direct push whenever it is the only buffered chunk, and
            /// it also guarantees progress: returning `PortFull` without having pushed
            /// anything would leave the executor with no port state change to wake us on.
            if (output.canPush())
                pushBufferedChunk();

            return Status::PortFull;
        }
        compactReplicatedColumns(chunk);
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

bool BufferChunksTransform::pushBufferedChunk()
{
    auto chunk = std::move(chunks.front());
    chunks.pop();

    num_buffered_rows -= chunk.getNumRows();
    num_buffered_bytes -= chunk.bytes();

    const bool virtual_row = isVirtualRow(chunk);
    output.push(std::move(chunk));

    if (virtual_row)
    {
        /// Stop reading until downstream has consumed the virtual-row marker, otherwise we
        /// would pull real chunks past the part boundary and defeat the LIMIT/read-in-order
        /// optimizations.
        wait_for_demand_after_virtual_row = true;
        input.setNotNeeded();
    }

    return virtual_row;
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
