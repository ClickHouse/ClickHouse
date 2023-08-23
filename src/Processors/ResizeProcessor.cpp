#include <Processors/ResizeProcessor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ResizeProcessor::Status ResizeProcessor::prepare()
{
    bool is_first_output = true;
    auto output_end = current_output;

    bool all_outs_full_or_unneeded = true;
    bool all_outs_finished = true;

    bool is_first_input = true;
    auto input_end = current_input;

    bool all_inputs_finished = true;

    auto is_end_input = [&]() { return !is_first_input && current_input == input_end; };
    auto is_end_output = [&]() { return !is_first_output && current_output == output_end; };

    auto inc_current_input = [&]()
    {
        is_first_input = false;
        ++current_input;

        if (current_input == inputs.end())
            current_input = inputs.begin();
    };

    auto inc_current_output = [&]()
    {
        is_first_output = false;
        ++current_output;

        if (current_output == outputs.end())
            current_output = outputs.begin();
    };

    /// Find next output where can push.
    auto get_next_out = [&, this]() -> OutputPorts::iterator
    {
        while (!is_end_output())
        {
            if (!current_output->isFinished())
            {
                all_outs_finished = false;

                if (current_output->canPush())
                {
                    all_outs_full_or_unneeded = false;
                    auto res_output = current_output;
                    inc_current_output();
                    return res_output;
                }
            }

            inc_current_output();
        }

        return outputs.end();
    };

    /// Find next input from where can pull.
    auto get_next_input = [&, this]() -> InputPorts::iterator
    {
        while (!is_end_input())
        {
            if (!current_input->isFinished())
            {
                all_inputs_finished = false;

                current_input->setNeeded();
                if (current_input->hasData())
                {
                    auto res_input = current_input;
                    inc_current_input();
                    return res_input;
                }
            }

            inc_current_input();
        }

        return inputs.end();
    };

    auto get_status_if_no_outputs = [&]() -> Status
    {
        if (all_outs_finished)
        {
            for (auto & in : inputs)
                in.close();

            return Status::Finished;
        }

        if (all_outs_full_or_unneeded)
        {
            for (auto & in : inputs)
                in.setNotNeeded();

            return Status::PortFull;
        }

        /// Now, we pushed to output, and it must be full.
        return Status::PortFull;
    };

    auto get_status_if_no_inputs = [&]() -> Status
    {
        if (all_inputs_finished)
        {
            for (auto & out : outputs)
                out.finish();

            return Status::Finished;
        }

        return Status::NeedData;
    };

    /// Set all inputs needed in order to evenly process them.
    /// Otherwise, in case num_outputs < num_inputs and chunks are consumed faster than produced,
    ///   some inputs can be skipped.
//    auto set_all_unprocessed_inputs_needed = [&]()
//    {
//        for (; cur_input != inputs.end(); ++cur_input)
//            if (!cur_input->isFinished())
//                cur_input->setNeeded();
//    };

    while (!is_end_input() && !is_end_output())
    {
        auto output = get_next_out();

        if (output == outputs.end())
            return get_status_if_no_outputs();

        auto input = get_next_input();

        if (input == inputs.end())
            return get_status_if_no_inputs();

        output->push(input->pull());
    }

    if (is_end_input())
        return get_status_if_no_outputs();

    /// cur_input == inputs_end()
    return get_status_if_no_inputs();
}

IProcessor::Status ResizeProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_ports.push_back({.port = &input, .status = InputStatus::NotActive});

        for (auto & output : outputs)
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
    }

    for (const auto & output_number : updated_outputs)
    {
        auto & output = output_ports[output_number];
        if (output.port->isFinished())
        {
            if (output.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                output.status = OutputStatus::Finished;
            }

            continue;
        }

        if (output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;
                waiting_outputs.push(output_number);
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

    for (const auto & input_number : updated_inputs)
    {
        auto & input = input_ports[input_number];
        if (input.port->isFinished())
        {
            if (input.status != InputStatus::Finished)
            {
                input.status = InputStatus::Finished;
                ++num_finished_inputs;
            }
            continue;
        }

        if (input.port->hasData())
        {
            if (input.status != InputStatus::HasData)
            {
                input.status = InputStatus::HasData;
                inputs_with_data.push(input_number);
            }
        }
    }

    while (!waiting_outputs.empty() && !inputs_with_data.empty())
    {
        auto & waiting_output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        auto & input_with_data = input_ports[inputs_with_data.front()];
        inputs_with_data.pop();

        waiting_output.port->pushData(input_with_data.port->pullData());
        input_with_data.status = InputStatus::NotActive;
        waiting_output.status = OutputStatus::NotActive;

        if (input_with_data.port->isFinished())
        {
            input_with_data.status = InputStatus::Finished;
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

IProcessor::Status StrictResizeProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_ports.push_back({.port = &input, .status = InputStatus::NotActive, .waiting_output = -1});

        for (UInt64 i = 0; i < input_ports.size(); ++i)
            disabled_input_ports.push(i);

        for (auto & output : outputs)
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
    }

    for (const auto & output_number : updated_outputs)
    {
        auto & output = output_ports[output_number];
        if (output.port->isFinished())
        {
            if (output.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                output.status = OutputStatus::Finished;
            }

            continue;
        }

        if (output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;
                waiting_outputs.push(output_number);
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    std::queue<UInt64> inputs_with_data;

    for (const auto & input_number : updated_inputs)
    {
        auto & input = input_ports[input_number];
        if (input.port->isFinished())
        {
            if (input.status != InputStatus::Finished)
            {
                input.status = InputStatus::Finished;
                ++num_finished_inputs;

                waiting_outputs.push(input.waiting_output);
            }
            continue;
        }

        if (input.port->hasData())
        {
            if (input.status != InputStatus::NotActive)
            {
                input.status = InputStatus::NotActive;
                inputs_with_data.push(input_number);
            }
        }
    }

    while (!inputs_with_data.empty())
    {
        auto input_number = inputs_with_data.front();
        auto & input_with_data = input_ports[input_number];
        inputs_with_data.pop();

        if (input_with_data.waiting_output == -1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No associated output for input with data");

        auto & waiting_output = output_ports[input_with_data.waiting_output];

        if (waiting_output.status == OutputStatus::NotActive)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid status NotActive for associated output");

        if (waiting_output.status != OutputStatus::Finished)
        {
            waiting_output.port->pushData(input_with_data.port->pullData(/* set_not_needed = */ true));
            waiting_output.status = OutputStatus::NotActive;
        }
        else
            abandoned_chunks.emplace_back(input_with_data.port->pullData(/* set_not_needed = */ true));

        if (input_with_data.port->isFinished())
        {
            input_with_data.status = InputStatus::Finished;
            ++num_finished_inputs;
        }
        else
            disabled_input_ports.push(input_number);
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
        auto & waiting_output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        waiting_output.port->pushData(std::move(abandoned_chunks.back()));
        abandoned_chunks.pop_back();

        waiting_output.status = OutputStatus::NotActive;
    }

    /// Enable more inputs if needed.
    while (!disabled_input_ports.empty() && !waiting_outputs.empty())
    {
        auto & input = input_ports[disabled_input_ports.front()];
        disabled_input_ports.pop();

        input.port->setNeeded();
        input.status = InputStatus::NeedData;
        input.waiting_output = waiting_outputs.front();

        waiting_outputs.pop();
    }

    /// Close all other waiting for data outputs (there is no corresponding input for them).
    while (!waiting_outputs.empty())
    {
        auto & output = output_ports[waiting_outputs.front()];
        waiting_outputs.pop();

        if (output.status != OutputStatus::Finished)
           ++num_finished_outputs;

        output.status = OutputStatus::Finished;
        output.port->finish();
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
