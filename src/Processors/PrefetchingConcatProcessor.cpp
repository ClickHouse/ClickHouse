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

    if (!output.isNeeded())
        return Status::PortFull;

    /// Pull available data from ALL inputs into their buffers.
    /// This lets their upstream sources continue producing data in parallel.
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (input.hasData())
                buffers[idx].push_back(input.pull());
            ++idx;
        }
    }

    /// Mark non-current inputs as needed if their buffer has room.
    /// This is the key to parallelism: the pipeline executor will schedule
    /// upstream sources for all "needed" inputs simultaneously.
    {
        size_t idx = 0;
        for (auto & input : inputs)
        {
            if (idx != current_input_idx && !input.isFinished() && buffers[idx].size() < max_buffered_chunks)
                input.setNeeded();
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
