#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Transforms/SelectorInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

IMergingTransformBase::IMergingTransformBase(
    size_t num_inputs,
    const Block & input_header,
    const Block & output_header,
    bool have_all_inputs_)
    : IProcessor(InputPorts(num_inputs, input_header), {output_header})
    , have_all_inputs(have_all_inputs_)
{
}

void IMergingTransformBase::onNewInput()
{
    throw Exception("onNewInput is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void IMergingTransformBase::addInput()
{
    if (have_all_inputs)
        throw Exception("IMergingTransform already have all inputs.", ErrorCodes::LOGICAL_ERROR);

    inputs.emplace_back(outputs.front().getHeader(), this);
    onNewInput();
}

void IMergingTransformBase::setHaveAllInputs()
{
    if (have_all_inputs)
        throw Exception("IMergingTransform already have all inputs.", ErrorCodes::LOGICAL_ERROR);

    have_all_inputs = true;
}

IProcessor::Status IMergingTransformBase::prepareInitializeInputs()
{
    /// Add information about inputs.
    if (input_states.empty())
    {
        input_states.reserve(inputs.size());
        for (auto & input : inputs)
            input_states.emplace_back(input);

        state.init_chunks.resize(inputs.size());
    }

    /// Check for inputs we need.
    bool all_inputs_has_data = true;
    auto it = inputs.begin();
    for (size_t i = 0; it != inputs.end(); ++i, ++it)
    {
        auto & input = *it;
        if (input.isFinished())
            continue;

        if (input_states[i].is_initialized)
        {
            // input.setNotNeeded();
            continue;
        }

        input.setNeeded();

        if (!input.hasData())
        {
            all_inputs_has_data = false;
            continue;
        }

        auto chunk = input.pull();
        if (!chunk.hasRows())
        {

            if (!input.isFinished())
                all_inputs_has_data = false;

            continue;
        }

        state.init_chunks[i] = std::move(chunk);
        input_states[i].is_initialized = true;
    }

    if (!all_inputs_has_data)
        return Status::NeedData;

    is_initialized = true;
    return Status::Ready;
}

IProcessor::Status IMergingTransformBase::prepare()
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

    /// Do not disable inputs, so it will work in the same way as with AsynchronousBlockInputStream, like before.
    bool is_port_full = !output.canPush();

    /// Push if has data.
    if (state.output_chunk && !is_port_full)
        output.push(std::move(state.output_chunk));

    if (!is_initialized)
        return prepareInitializeInputs();

    if (state.is_finished)
    {
        if (is_port_full)
            return Status::PortFull;

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

            state.input_chunk = input.pull();
            if (!state.input_chunk.hasRows() && !input.isFinished())
                return Status::NeedData;

            state.has_input = true;
        }

        state.need_data = false;
    }

    if (is_port_full)
        return Status::PortFull;

    return Status::Ready;
}

static void filterChunk(Chunk & chunk, size_t selector_position)
{
    if (!chunk.getChunkInfo())
        throw Exception("IMergingTransformBase expected ChunkInfo for input chunk", ErrorCodes::LOGICAL_ERROR);

    const auto * chunk_info = typeid_cast<const SelectorInfo *>(chunk.getChunkInfo().get());
    if (!chunk_info)
        throw Exception("IMergingTransformBase expected SelectorInfo for input chunk", ErrorCodes::LOGICAL_ERROR);

    const auto & selector = chunk_info->selector;

    IColumn::Filter filter;
    filter.resize_fill(selector.size());

    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    size_t num_result_rows = 0;

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (selector[row] == selector_position)
        {
            ++num_result_rows;
            filter[row] = 1;
        }
    }

    for (auto & column : columns)
        column = column->filter(filter, num_result_rows);

    chunk.clear();
    chunk.setColumns(std::move(columns), num_result_rows);
}

bool IMergingTransformBase::filterChunks()
{
    if (state.selector_position < 0)
        return true;

    bool has_empty_chunk = false;

    if (!state.init_chunks.empty())
    {
        for (size_t i = 0; i < input_states.size(); ++i)
        {
            auto & chunk = state.init_chunks[i];
            if (!chunk || input_states[i].is_filtered)
                continue;

            filterChunk(chunk, state.selector_position);

            if (!chunk.hasRows())
            {
                chunk.clear();
                has_empty_chunk = true;
                input_states[i].is_initialized = false;
                is_initialized = false;
            }
            else
                input_states[i].is_filtered = true;
        }
    }

    if (state.has_input)
    {
        filterChunk(state.input_chunk, state.selector_position);
        if (!state.input_chunk.hasRows())
        {
            state.has_input = false;
            state.need_data = true;
            has_empty_chunk = true;
        }
    }

    return !has_empty_chunk;
}


}
