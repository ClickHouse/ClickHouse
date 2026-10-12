#include <Processors/PrefetchingConcatProcessor.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

namespace DB
{

PrefetchingConcatProcessor::PrefetchingConcatProcessor(
    SharedHeader header, size_t num_inputs, size_t max_rows_to_buffer_, size_t max_bytes_to_buffer_, size_t max_prefetch_inputs_)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts{header})
    , max_rows_to_buffer(max_rows_to_buffer_)
    , max_bytes_to_buffer(max_bytes_to_buffer_)
    , max_prefetch_inputs(max_prefetch_inputs_)
    , buffers(num_inputs)
{
}

PrefetchingConcatProcessor::Status PrefetchingConcatProcessor::prepare()
{
    auto & output = outputs.front();

    /// Output finished — close everything.
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    /// After a virtual row was forwarded, the downstream merge may defer this stream until it
    /// reaches the key from the virtual row. Until it demands data again, keep every input
    /// stopped: neither the current input nor the prefetch window may read past the announced
    /// boundary, otherwise each deferred stream would read speculatively (the same contract as
    /// in `BufferChunksTransform`).
    if (wait_for_demand_after_virtual_row)
    {
        if (!output.canPush())
        {
            stopAllInputs();
            return Status::PortFull;
        }

        wait_for_demand_after_virtual_row = false;
        demanded = true;
    }

    if (!output.canPush())
        return Status::PortFull;

    /// Pull available data from inputs into their buffers.
    /// This lets their upstream sources continue producing data in parallel.
    ///
    /// A non-current input is only drained while its own buffer is below the budget. Once the
    /// budget is spent we stop pulling, so the buffer honors it even for a chunk still in
    /// flight on the port after a previous `setNotNeeded` (ports hold one chunk, which would
    /// otherwise push the buffer one past the limit). The current input is always drained
    /// because its chunks are emitted immediately and never accumulate.
    ///
    /// A chunk can be in flight on a port that was made not needed since (the budget was spent
    /// or the inputs were stopped after a virtual row), so pull with `set_not_needed`: it is
    /// allowed for a port that is not needed and leaves it not needed. Whether the input is
    /// needed is decided below on every call.
    ///
    /// Only the first input keeps its virtual row: it announces the boundary of the whole
    /// concatenated stream. Every other input starts where the previous one ends, so its own
    /// leading virtual row tells downstream nothing it could defer on. If it were buffered, it
    /// would park the input on its boundary for as long as it is in the prefetch window, and
    /// since every stream starts with one, nothing would ever be prefetched. Drop it instead.
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (input.hasData()
                && (idx == current_input_idx || buffers[idx].hasRoom(max_rows_to_buffer, max_bytes_to_buffer)))
            {
                auto chunk = input.pull(/* set_not_needed = */ true);
                if (idx == 0 || !isVirtualRow(chunk))
                    buffers[idx].push(std::move(chunk));
            }
            ++idx;
        }
    }

    /// Mark a limited number of upcoming inputs as needed for prefetching.
    /// Only prefetch the next few inputs (not all) to limit overhead:
    /// running too many sources in parallel wastes CPU on decompression and
    /// filtering for data that won't be consumed until much later.
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (idx != current_input_idx && !input.isFinished())
            {
                bool should_prefetch = demanded
                    && idx > current_input_idx
                    && idx <= current_input_idx + max_prefetch_inputs
                    && buffers[idx].hasRoom(max_rows_to_buffer, max_bytes_to_buffer);

                if (should_prefetch)
                    input.setNeeded();
                else
                    input.setNotNeeded();
            }
            ++idx;
        }
    }

    /// Skip finished inputs with empty buffers.
    {
        auto it = inputs.begin();
        std::advance(it, current_input_idx);
        while (it != inputs.end() && it->isFinished() && buffers[current_input_idx].chunks.empty())
        {
            ++it;
            ++current_input_idx;
        }
    }

    if (current_input_idx >= buffers.size())
    {
        output.finish();
        return Status::Finished;
    }

    /// Try to output from the current input's buffer.
    if (!buffers[current_input_idx].chunks.empty())
    {
        auto chunk = buffers[current_input_idx].pop();
        const bool virtual_row = isVirtualRow(chunk);
        output.push(std::move(chunk));

        if (virtual_row)
        {
            wait_for_demand_after_virtual_row = true;
            stopAllInputs();
        }
        else
        {
            demanded = true;

            /// Keep the current input reading while downstream consumes this chunk, unless a
            /// virtual row is still buffered: nothing may be read behind it before it is forwarded.
            auto it = inputs.begin();
            std::advance(it, current_input_idx);
            if (!it->isFinished() && buffers[current_input_idx].num_virtual_rows == 0)
                it->setNeeded();
        }

        return Status::PortFull;
    }

    /// Buffer is empty — request data from the current input.
    {
        auto it = inputs.begin();
        std::advance(it, current_input_idx);
        it->setNeeded();
    }

    return Status::NeedData;
}

void PrefetchingConcatProcessor::Buffer::push(Chunk && chunk)
{
    num_rows += chunk.getNumRows();
    num_bytes += chunk.bytes();
    if (isVirtualRow(chunk))
        ++num_virtual_rows;
    chunks.push_back(std::move(chunk));
}

Chunk PrefetchingConcatProcessor::Buffer::pop()
{
    auto chunk = std::move(chunks.front());
    chunks.pop_front();
    num_rows -= chunk.getNumRows();
    num_bytes -= chunk.bytes();
    if (isVirtualRow(chunk))
        --num_virtual_rows;
    return chunk;
}

void PrefetchingConcatProcessor::stopAllInputs()
{
    for (auto & input : inputs)
        if (!input.isFinished())
            input.setNotNeeded();
}

}
