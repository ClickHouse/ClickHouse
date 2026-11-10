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
                state.is_waiting = true;
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

                /// Release the output this input was paired with, so that it can be handed to
                /// another input or closed. Only an output that is free right now may be released:
                /// an output in the `NotActive` state still holds a chunk that the downstream
                /// processor has not consumed yet, and handing it to another input would break the
                /// invariant checked below. Such an output is enqueued by the loop over
                /// `updated_outputs` above as soon as it becomes pushable again. An input that was
                /// never paired with an output has no output to release.
                if (state.waiting_output)
                {
                    auto & released_state = output_port_state.at(state.waiting_output);
                    if (released_state.status == OutputStatus::NeedData && !released_state.is_waiting)
                    {
                        released_state.is_waiting = true;
                        waiting_outputs.push(state.waiting_output);
                    }
                }
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

        /// The pairing is consumed by this chunk. If the pointer were kept, a later `isFinished`
        /// on this input would release an output that has meanwhile been handed to another input.
        input_state.waiting_output = nullptr;

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

    /// `waiting_outputs` is a lazy queue: an output may have finished after it was enqueued (the
    /// loop over `updated_outputs` above flips its status but cannot remove it from the middle of
    /// the queue), so every consumer below re-checks the current status and skips stale entries.

    /// Process abandoned chunks if any.
    while (!abandoned_chunks.empty() && !waiting_outputs.empty())
    {
        auto * waiting_output = waiting_outputs.front();
        auto & output_state = output_port_state.at(waiting_output);
        waiting_outputs.pop();
        output_state.is_waiting = false;

        if (output_state.status != OutputStatus::NeedData)
            continue;

        waiting_output->pushData(std::move(abandoned_chunks.back()));
        abandoned_chunks.pop_back();

        output_state.status = OutputStatus::NotActive;
    }

    /// Enable more inputs if needed.
    /// `disabled_input_ports` is a lazy queue as well: an input may finish while it is sitting in
    /// the queue, because an upstream output is allowed to `finish` even when the peer input is not
    /// needed. The loop over `updated_inputs` above flips such an input to `Finished` but cannot
    /// remove it from the middle of the queue, so stale entries are skipped here. Pairing an output
    /// with an already finished input would strand the output forever and could deadlock the
    /// processor, and it would also let the input be counted as finished twice.
    while (!disabled_input_ports.empty() && !waiting_outputs.empty())
    {
        auto * input_port = disabled_input_ports.front();
        auto & input_state = input_port_state.at(input_port);

        if (input_state.status == InputStatus::Finished)
        {
            disabled_input_ports.pop();
            continue;
        }

        auto * waiting_output = waiting_outputs.front();
        auto & output_state = output_port_state.at(waiting_output);
        waiting_outputs.pop();
        output_state.is_waiting = false;

        if (output_state.status != OutputStatus::NeedData)
            continue;

        disabled_input_ports.pop();

        input_port->setNeeded();
        input_state.status = InputStatus::NeedData;
        input_state.waiting_output = waiting_output;
    }

    /// Close all other waiting for data outputs (there is no corresponding input for them).
    while (!waiting_outputs.empty())
    {
        auto * output_port = waiting_outputs.front();
        auto & output_state = output_port_state.at(output_port);
        waiting_outputs.pop();
        output_state.is_waiting = false;

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
