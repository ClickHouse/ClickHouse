#include <Processors/PrefetchingConcatProcessor.h>
#include <Processors/Port.h>

namespace DB
{

PrefetchingConcatProcessor::PrefetchingConcatProcessor(
    SharedHeader header, size_t num_inputs, size_t max_buffered_bytes_, size_t min_buffered_chunks_)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts{header})
    , max_buffered_bytes(max_buffered_bytes_)
    , min_buffered_chunks(min_buffered_chunks_)
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

    /// Pull available data from inputs into their buffers and toggle each input's `needed`
    /// state based on whether its buffer has room. Setting `needed` on inputs with room is
    /// what drives parallel prefetching: the pipeline executor schedules the upstream sources
    /// for all "needed" inputs simultaneously. Clearing `needed` on full buffers is what stops
    /// upstream from over-producing. The current input is included so its upstream keeps
    /// producing into the buffer rather than stalling between consecutive `pull` calls.
    ///
    /// The inputs share the budget of `max_buffered_bytes`: an input has room while all buffers
    /// together hold less than it. The buffers of later inputs stay resident until the current
    /// input is drained, so all of them must be counted, otherwise the total would grow with
    /// the number of inputs. The inputs are visited in order starting from the current one,
    /// so the nearest inputs, which are needed first, get the freed budget first. The current
    /// input (or every input if `max_buffered_bytes` is 0) also has room while it buffers fewer
    /// than `min_buffered_chunks` chunks. The inputs before `current_input_idx` are finished
    /// and drained.
    ///
    /// `pull(set_not_needed=true)` is required because the input may legitimately
    /// have `IS_NEEDED == 0` here: an upstream chunk can race with our previous
    /// `setNotNeeded` (e.g. set when the output became not-needed, or when the
    /// buffer hit capacity), arriving after the flag was cleared.
    auto pull_inputs = [&]
    {
        size_t total_buffered_bytes = 0;
        for (const auto & buffer : buffers)
            total_buffered_bytes += buffer.bytes;

        auto it = inputs.begin();
        std::advance(it, current_input_idx);
        for (size_t idx = current_input_idx; idx < buffers.size(); ++idx, ++it)
        {
            auto & input = *it;
            auto & buffer = buffers[idx];
            /// The head start of `min_buffered_chunks` is not counted in the budget only for the
            /// current input, which must make progress, or for every input when there is no budget.
            /// Otherwise the buffered bytes would grow with the number of inputs regardless of it.
            const bool head_start = idx == current_input_idx || max_buffered_bytes == 0;
            auto has_room = [&]
            {
                return (head_start && buffer.chunks.size() < min_buffered_chunks)
                    || total_buffered_bytes < max_buffered_bytes;
            };

            if (has_room() && input.hasData())
            {
                auto chunk = input.pull(true);
                const size_t chunk_bytes = chunk.allocatedBytes();
                buffer.bytes += chunk_bytes;
                total_buffered_bytes += chunk_bytes;
                buffer.chunks.push_back(std::move(chunk));
            }

            if (!input.isFinished())
            {
                if (has_room())
                    input.setNeeded();
                else
                    input.setNotNeeded();
            }
        }
    };

    auto current_input_is_drained = [&]
    {
        auto it = inputs.begin();
        std::advance(it, current_input_idx);
        return it->isFinished() && buffers[current_input_idx].chunks.empty();
    };

    /// Skip finished inputs with empty buffers. The inputs must be pulled again after the current input
    /// changes: the new current input has the head start, and its data may already wait in the port
    /// while it had no room before. Returning `NeedData` without pulling it would hang the pipeline,
    /// because no new data would arrive to wake this processor up.
    while (true)
    {
        while (current_input_idx < buffers.size() && current_input_is_drained())
            ++current_input_idx;

        if (current_input_idx >= buffers.size())
        {
            output.finish();
            return Status::Finished;
        }

        pull_inputs();

        if (!current_input_is_drained())
            break;
    }

    /// Try to output from the current input's buffer.
    if (!output.canPush())
        return Status::PortFull;

    if (!buffers[current_input_idx].chunks.empty())
    {
        auto & buffer = buffers[current_input_idx];
        auto chunk = std::move(buffer.chunks.front());
        buffer.chunks.pop_front();
        buffer.bytes -= std::min<size_t>(buffer.bytes, chunk.allocatedBytes());
        output.push(std::move(chunk));
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
