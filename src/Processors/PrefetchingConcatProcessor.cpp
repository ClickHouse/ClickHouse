#include <Processors/PrefetchingConcatProcessor.h>
#include <Processors/Port.h>

namespace DB
{

PrefetchingConcatProcessor::PrefetchingConcatProcessor(SharedHeader header, size_t num_inputs, size_t max_buffered_chunks_)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts{header})
    , max_buffered_chunks(max_buffered_chunks_)
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

    /// Output is not needed downstream — propagate backpressure to inputs so
    /// upstream sources stop producing while we wait. Without this, inputs
    /// remain in their previous `needed` state and buffers grow indefinitely.
    if (!output.isNeeded())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    /// Pull available data from inputs into their buffers, capped per input.
    /// Pulling when the buffer is at capacity would let upstream produce
    /// faster than we can consume, growing memory without bound.
    ///
    /// `pull(set_not_needed=true)` is required because the input may legitimately
    /// have `IS_NEEDED == 0` here: an upstream chunk can race with our previous
    /// `setNotNeeded` (e.g. set when the output became not-needed, or when the
    /// buffer hit capacity), arriving after the flag was cleared. The toggle
    /// loop below will re-set `IS_NEEDED` based on the current buffer state.
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (buffers[idx].size() < max_buffered_chunks && input.hasData())
                buffers[idx].push_back(input.pull(true));
            ++idx;
        }
    }

    /// Toggle each input's `needed` state based on whether its buffer has
    /// room. Setting `needed` on inputs with capacity is what drives parallel
    /// prefetching — the pipeline executor schedules the upstream sources for
    /// all "needed" inputs simultaneously. Clearing `needed` on full buffers
    /// is what stops upstream from over-producing. The current input is
    /// included so its upstream keeps producing into the buffer rather than
    /// stalling between consecutive `pull` calls.
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (!input.isFinished())
            {
                if (buffers[idx].size() < max_buffered_chunks)
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
        while (it != inputs.end() && it->isFinished() && buffers[current_input_idx].empty())
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
    if (!output.canPush())
        return Status::PortFull;

    if (!buffers[current_input_idx].empty())
    {
        output.push(std::move(buffers[current_input_idx].front()));
        buffers[current_input_idx].pop_front();
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
