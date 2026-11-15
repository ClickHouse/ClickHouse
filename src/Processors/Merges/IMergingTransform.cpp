#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

IMergingTransformBase::IMergingTransformBase(
    size_t num_inputs,
    SharedHeader & input_header,
    SharedHeader & output_header,
    bool have_all_inputs_,
    UInt64 limit_hint_,
    bool always_read_till_end_)
    : IProcessor(InputPorts(num_inputs, input_header), {output_header})
    , have_all_inputs(have_all_inputs_)
    , limit_hint(limit_hint_)
    , always_read_till_end(always_read_till_end_)
{
}

OutputPort & IMergingTransformBase::getOutputPort()
{
    return outputs.front();
}

static InputPorts createPorts(const SharedHeaders & blocks)
{
    InputPorts ports;
    for (const auto & block : blocks)
        ports.emplace_back(block);
    return ports;
}

IMergingTransformBase::IMergingTransformBase(
    SharedHeaders & input_headers,
    SharedHeader & output_header,
    bool have_all_inputs_,
    UInt64 limit_hint_,
    bool always_read_till_end_)
    : IProcessor(createPorts(input_headers), {output_header})
    , have_all_inputs(have_all_inputs_)
    , limit_hint(limit_hint_)
    , always_read_till_end(always_read_till_end_)
{
}

void IMergingTransformBase::onNewInput()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "onNewInput is not implemented for {}", getName());
}

void IMergingTransformBase::addInput(const Block & input_header)
{
    if (have_all_inputs)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IMergingTransform already have all inputs.");

    inputs.emplace_back(input_header, this);
    onNewInput();
}

void IMergingTransformBase::setHaveAllInputs()
{
    if (have_all_inputs)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IMergingTransform already have all inputs.");

    have_all_inputs = true;
}

bool IMergingTransformBase::tryInitializeInput(size_t input_num)
{
    auto & input_state = input_states[input_num];
    auto & input = input_state.port;
    if (input.isFinished())
    {
        input_state.is_initialized = true;
        return true;
    }

    input.setNeeded();

    if (!input.hasData())
        return false;

    /// setNotNeeded after reading first chunk, because in optimismtic case
    /// (e.g. with optimized 'ORDER BY primary_key LIMIT n' and small 'n')
    /// we won't have to read any chunks anymore;
    /// If virtual row exists, let it pass through, so don't read more chunks.
    auto chunk = input.pull(true);
    bool virtual_row = isVirtualRow(chunk);
    if (limit_hint == 0 && !virtual_row)
        input.setNeeded();

    if (!virtual_row && limit_hint && chunk.getNumRows() < limit_hint)
        input.setNeeded();

    /// With `always_read_till_end` every source is read in full regardless of where the
    /// merge stops, so deferring a source behind its virtual row cannot save any reads.
    /// It would only serialize them: a source left NotNeeded here does not start reading,
    /// and once the merge finishes, the drain in `prepare` wakes the leftover sources one
    /// at a time. Keep them producing concurrently from the start instead. (Currently the
    /// planner sets this flag only on merges over remote streams, which carry no virtual
    /// rows — see `addMergeSortingStep` — so this is enforcing the invariant locally
    /// rather than fixing a reachable case.)
    if (always_read_till_end)
        input.setNeeded();

    if (!virtual_row && !chunk.hasRows())
    {
        if (!input.isFinished())
        {
            input.setNeeded();
            return false;
        }

        input_state.is_initialized = true;
        return true;
    }

    state.init_chunks[input_num].set(std::move(chunk));
    input_state.is_initialized = true;
    return true;
}

IProcessor::Status IMergingTransformBase::prepareInitializeInputs(const UpdatedInputPorts * updated_inputs)
{
    /// The first call visits every input. Later calls visit only the inputs whose state changed,
    /// otherwise waiting for the first chunks of N inputs costs O(N^2).
    if (input_states.empty())
    {
        input_states.reserve(inputs.size());
        for (auto & input : inputs)
            input_states.emplace_back(input);

        input_port_to_num.reserve(inputs.size());
        for (size_t input_num = 0; input_num < input_states.size(); ++input_num)
            input_port_to_num.emplace_back(&input_states[input_num].port, input_num);
        std::ranges::sort(input_port_to_num);

        state.init_chunks.resize(inputs.size());
        num_uninitialized_inputs = inputs.size();
        updated_inputs = nullptr;
    }

    if (updated_inputs)
    {
        for (const auto * input : *updated_inputs)
        {
            auto it = std::ranges::lower_bound(input_port_to_num, input, {}, &std::pair<const InputPort *, size_t>::first);
            chassert(it != input_port_to_num.end() && it->first == input);
            size_t input_num = it->second;
            if (!input_states[input_num].is_initialized && tryInitializeInput(input_num))
                --num_uninitialized_inputs;
        }
    }
    else
    {
        for (size_t input_num = 0; input_num < input_states.size(); ++input_num)
        {
            if (!input_states[input_num].is_initialized && tryInitializeInput(input_num))
                --num_uninitialized_inputs;
        }
    }

    if (num_uninitialized_inputs)
        return Status::NeedData;

    input_port_to_num = {};
    is_initialized = true;
    return Status::Ready;
}

IProcessor::Status IMergingTransformBase::prepare()
{
    return prepareImpl(nullptr);
}

IProcessor::Status IMergingTransformBase::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & /*updated_outputs*/)
{
    return prepareImpl(&updated_inputs);
}

IProcessor::Status IMergingTransformBase::prepareImpl(const UpdatedInputPorts * updated_inputs)
{
    if (!have_all_inputs)
        return Status::NeedData;

    auto & output = outputs.front();

    /// Special case for no inputs.
    if (inputs.empty())
    {
        output.finish();
        onFinish();
        return Status::Finished;
    }

    /// Check can output.

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();

        onFinish();
        return Status::Finished;
    }

    /// Do not disable inputs, so they can be executed in parallel.
    bool is_port_full = !output.canPush();

    /// Push if has data.
    if ((state.output_chunk || !state.output_chunk.getChunkInfos().empty()) && !is_port_full)
        output.push(std::move(state.output_chunk));

    if (!is_initialized)
        return prepareInitializeInputs(updated_inputs);

    if (!state.inputs_to_prefetch.empty())
    {
        if (state.is_finished)
        {
            /// The merge finished before the read-ahead could start: the data would
            /// never be consumed.
            state.inputs_to_prefetch.clear();
        }
        else
        {
            /// Ask the inputs deferred behind virtual rows to start producing data without
            /// waiting for it, so that they read in parallel with the merge.
            for (size_t input_num : state.inputs_to_prefetch)
            {
                auto & input = input_states[input_num].port;
                if (!input.isFinished())
                    input.setNeeded();
            }
            state.inputs_to_prefetch.clear();
        }
    }

    if (state.is_finished)
    {
        if (is_port_full)
            return Status::PortFull;

        if (always_read_till_end)
        {
            for (auto & input : inputs)
            {
                if (!input.isFinished())
                {
                    input.setNeeded();
                    if (input.hasData())
                        std::ignore = input.pull();

                    return Status::NeedData;
                }
            }
        }

        for (auto & input : inputs)
            input.close();

        outputs.front().finish();

        onFinish();
        return Status::Finished;
    }

    if (state.need_data)
    {
        auto & input = input_states[state.next_input_to_read].port;
        if (!input.isFinished())
        {
            input.setNeeded();

            if (!input.hasData())
                return Status::NeedData;

            state.input_chunk.set(input.pull(/* set_not_needed */ true));
            const auto & input_chunk = state.input_chunk.chunk;

            bool virtual_row = isVirtualRow(input_chunk);
            if (!virtual_row && (!limit_hint || input_chunk.getNumRows() < limit_hint || always_read_till_end))
            {
                input.setNeeded();
            }
            if (!input_chunk.hasRows() && !virtual_row)
            {
                if (!input.isFinished())
                {
                    input.setNeeded();
                    return Status::NeedData;
                }

                /// The input finished with an empty chunk: there is nothing to consume.
                /// Passing it to the algorithm would put an empty cursor into the sorting
                /// queue (`Logical error: 'max_rows > 0'` in the batch merge, out-of-bounds
                /// row access in the heap comparisons). Report the source as exhausted
                /// instead, like the initialization path does for empty first chunks.
                state.input_chunk.set(Chunk());
                state.no_data = true;
            }
            else
            {
                state.has_input = true;
            }
        }
        else
        {
            state.no_data = true;
        }

        state.need_data = false;
    }

    if (is_port_full)
        return Status::PortFull;

    return Status::Ready;
}

}
