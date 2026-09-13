#include <Processors/PrefetchingConcatProcessor.h>
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
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (input.hasData()
                && (idx == current_input_idx || buffers[idx].hasRoom(max_rows_to_buffer, max_bytes_to_buffer)))
                buffers[idx].push(input.pull());
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
                bool should_prefetch = idx > current_input_idx
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
        output.push(buffers[current_input_idx].pop());
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

}
