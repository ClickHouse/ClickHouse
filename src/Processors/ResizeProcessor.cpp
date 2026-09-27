#include <Processors/ResizeProcessor.h>

#include <Processors/Port.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// TODO Check that there is non zero number of inputs and outputs.
ResizeProcessor::ResizeProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
    , current_input(inputs.begin())
    , current_output(outputs.begin())
{
}

IProcessor::Status ResizeProcessor::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_status[&input] = InputStatus::NotActive;

        for (auto & output : outputs)
            output_status[&output] = OutputStatus::NotActive;
    }

    for (auto * output_port : updated_outputs)
    {
        OutputStatus & status = output_status.at(output_port);
        if (output_port->isFinished())
        {
            if (status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                status = OutputStatus::Finished;
            }

            continue;
        }

        if (output_port->canPush())
        {
            if (status != OutputStatus::NeedData)
            {
                status = OutputStatus::NeedData;
                waiting_outputs.push(output_port);
            }
        }
    }

    if (!is_reading_started && !waiting_outputs.empty())
    {
        for (auto & input : inputs)
            input.setNeeded();
        is_reading_started = true;
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    for (auto * input_port : updated_inputs)
    {
        auto & status = input_status.at(input_port);
        if (input_port->isFinished())
        {
            if (status != InputStatus::Finished)
            {
                status = InputStatus::Finished;
                ++num_finished_inputs;
            }
            continue;
        }

        if (input_port->hasData())
        {
            if (status != InputStatus::HasData)
            {
                status = InputStatus::HasData;
                inputs_with_data.push(input_port);
            }
        }
    }

    while (!waiting_outputs.empty() && !inputs_with_data.empty())
    {
        auto * waiting_output = waiting_outputs.front();
        waiting_outputs.pop();

        auto * input_with_data = inputs_with_data.front();
        inputs_with_data.pop();

        waiting_output->pushData(input_with_data->pullData());
        input_status.at(input_with_data) = InputStatus::NotActive;
        output_status.at(waiting_output) = OutputStatus::NotActive;

        if (input_with_data->isFinished())
        {
            input_status.at(input_with_data) = InputStatus::Finished;
            ++num_finished_inputs;
        }
    }

    if (num_finished_inputs == inputs.size())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    if (!waiting_outputs.empty())
        return Status::NeedData;

    return Status::PortFull;
}

IProcessor::Status StrictResizeProcessor::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_port_state[&input] = {.status = InputStatus::NotActive, .waiting_output = nullptr};

        for (auto & input : inputs)
            disabled_input_ports.push(&input);

        for (auto & output : outputs)
            output_port_state[&output] = {.status = OutputStatus::NotActive};
    }

    for (auto * output_port : updated_outputs)
    {
        auto & state = output_port_state.at(output_port);
        if (output_port->isFinished())
        {
            if (state.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                state.status = OutputStatus::Finished;
            }

            continue;
        }

        if (output_port->canPush())
        {
            if (state.status != OutputStatus::NeedData)
            {
                state.status = OutputStatus::NeedData;
                waiting_outputs.push(output_port);
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    std::queue<InputPort *> inputs_with_data;

    for (auto * input_port : updated_inputs)
    {
        auto & state = input_port_state.at(input_port);
        if (input_port->isFinished())
        {
            if (state.status != InputStatus::Finished)
            {
                state.status = InputStatus::Finished;
                ++num_finished_inputs;

                waiting_outputs.push(state.waiting_output);
            }
            continue;
        }

        if (input_port->hasData())
        {
            if (state.status != InputStatus::NotActive)
            {
                state.status = InputStatus::NotActive;
                inputs_with_data.push(input_port);
            }
        }
    }

    while (!inputs_with_data.empty())
    {
        auto * input_port = inputs_with_data.front();
        auto & input_state = input_port_state.at(input_port);
        inputs_with_data.pop();

        if (input_state.waiting_output == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No associated output for input with data");

        auto * waiting_output = input_state.waiting_output;
        auto & output_state = output_port_state.at(waiting_output);

        if (output_state.status == OutputStatus::NotActive)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid status NotActive for associated output");

        if (output_state.status != OutputStatus::Finished)
        {
            waiting_output->pushData(input_port->pullData(/* set_not_needed = */ true));
            output_state.status = OutputStatus::NotActive;
        }
        else
            abandoned_chunks.emplace_back(input_port->pullData(/* set_not_needed = */ true));

        if (input_port->isFinished())
        {
            input_state.status = InputStatus::Finished;
            ++num_finished_inputs;
        }
        else
            disabled_input_ports.push(input_port);
    }

    if (num_finished_inputs == inputs.size())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    /// Process abandoned chunks if any.
    while (!abandoned_chunks.empty() && !waiting_outputs.empty())
    {
        auto * waiting_output = waiting_outputs.front();
        auto & output_state = output_port_state.at(waiting_output);
        waiting_outputs.pop();

        waiting_output->pushData(std::move(abandoned_chunks.back()));
        abandoned_chunks.pop_back();

        output_state.status = OutputStatus::NotActive;
    }

    /// Enable more inputs if needed.
    while (!disabled_input_ports.empty() && !waiting_outputs.empty())
    {
        auto * input_port = disabled_input_ports.front();
        auto & input_state = input_port_state.at(input_port);
        disabled_input_ports.pop();

        input_port->setNeeded();
        input_state.status = InputStatus::NeedData;
        input_state.waiting_output = waiting_outputs.front();

        waiting_outputs.pop();
    }

    /// Close all other waiting for data outputs (there is no corresponding input for them).
    while (!waiting_outputs.empty())
    {
        auto * output_port = waiting_outputs.front();
        auto & output_state = output_port_state.at(output_port);
        waiting_outputs.pop();

        if (output_state.status != OutputStatus::Finished)
           ++num_finished_outputs;

        output_state.status = OutputStatus::Finished;
        output_port->finish();
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (disabled_input_ports.empty())
        return Status::NeedData;

    return Status::PortFull;
}

}
