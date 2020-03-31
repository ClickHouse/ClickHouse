#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/MergedData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

IMergingTransform::IMergingTransform(
    size_t num_inputs,
    const Block & input_header,
    const Block & output_header,
    bool have_all_inputs_)
    : IProcessor(InputPorts(num_inputs, input_header), {output_header})
    , have_all_inputs(have_all_inputs_)
{
}

void IMergingTransform::onNewInput()
{
    throw Exception("onNewInput is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void IMergingTransform::addInput()
{
    if (have_all_inputs)
        throw Exception("IMergingTransform already have all inputs.", ErrorCodes::LOGICAL_ERROR);

    inputs.emplace_back(outputs.front().getHeader(), this);
    onNewInput();
}

void IMergingTransform::setHaveAllInputs()
{
    if (have_all_inputs)
        throw Exception("IMergingTransform already have all inputs.", ErrorCodes::LOGICAL_ERROR);

    have_all_inputs = true;
}

void IMergingTransform::requestDataForInput(size_t input_number)
{
    if (need_data)
        throw Exception("Data was requested for several inputs in IMergingTransform:"
                        " " + std::to_string(next_input_to_read) + " and " + std::to_string(input_number),
                        ErrorCodes::LOGICAL_ERROR);

    need_data = true;
    next_input_to_read = input_number;
}

void IMergingTransform::prepareOutputChunk(MergedData & merged_data)
{
    has_output_chunk = (is_finished && merged_data.mergedRows()) || merged_data.hasEnoughRows();
    if (has_output_chunk)
        output_chunk = merged_data.pull();
}

IProcessor::Status IMergingTransform::prepareSingleInput()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (input.isFinished())
    {
        output.finish();
        onFinish();
        return Status::Finished;
    }

    input.setNeeded();

    if (input.hasData())
    {
        if (output.canPush())
            output.push(input.pull());

        return Status::PortFull;
    }

    return Status::NeedData;
}

IProcessor::Status IMergingTransform::prepareInitializeInputs()
{
    /// Add information about inputs.
    if (input_states.empty())
    {
        input_states.reserve(inputs.size());
        for (auto & input : inputs)
            input_states.emplace_back(input);
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

        consume(std::move(chunk), i);
        input_states[i].is_initialized = true;
    }

    if (!all_inputs_has_data)
        return Status::NeedData;

    initializeInputs();

    is_initialized = true;
    return Status::Ready;
}


IProcessor::Status IMergingTransform::prepare()
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

    /// Special case for single input.
    if (inputs.size() == 1)
        return prepareSingleInput();

    /// Do not disable inputs, so it will work in the same way as with AsynchronousBlockInputStream, like before.
    bool is_port_full = !output.canPush();

    /// Push if has data.
    if (has_output_chunk && !is_port_full)
        output.push(std::move(output_chunk));

    if (!is_initialized)
        return prepareInitializeInputs();

    if (is_finished)
    {

        if (is_port_full)
            return Status::PortFull;

        for (auto & input : inputs)
            input.close();

        outputs.front().finish();

        onFinish();
        return Status::Finished;
    }

    if (need_data)
    {
        auto & input = input_states[next_input_to_read].port;
        if (!input.isFinished())
        {
            input.setNeeded();

            if (!input.hasData())
                return Status::NeedData;

            auto chunk = input.pull();
            if (!chunk.hasRows() && !input.isFinished())
                return Status::NeedData;

            consume(std::move(chunk), next_input_to_read);
        }

        need_data = false;
    }

    if (is_port_full)
        return Status::PortFull;

    return Status::Ready;
}

}
