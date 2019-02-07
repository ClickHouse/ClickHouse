#include <Processors/ResizeProcessor.h>


namespace DB
{

ResizeProcessor::Status ResizeProcessor::prepare()
{
    auto cur_output = outputs.begin();
    bool all_outs_full_or_unneeded = true;
    bool all_outs_finished = true;

    /// Find next output where can push.
    auto get_next_out = [&, this]() -> OutputPorts::iterator
    {
        while (cur_output != outputs.end())
        {
            if (!cur_output->isFinished())
            {
                all_outs_finished = false;

                if (cur_output->canPush())
                {
                    all_outs_full_or_unneeded = false;
                    ++cur_output;
                    return std::prev(cur_output);
                }
            }

            ++cur_output;
        }

        return cur_output;
    };

    auto cur_input = inputs.begin();
    bool all_inputs_finished = true;

    /// Find next input from where can pull.
    auto get_next_input = [&, this]() -> InputPorts::iterator
    {
        while (cur_input != inputs.end())
        {
            if (!cur_input->isFinished())
            {
                all_inputs_finished = false;

                cur_input->setNeeded();
                if (cur_input->hasData())
                {
                    ++cur_input;
                    return std::prev(cur_input);
                }
            }

            ++cur_input;
        }

        return cur_input;
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

    while (cur_input != inputs.end() && cur_output != outputs.end())
    {
        auto output = get_next_out();
        if (output == outputs.end())
            return get_status_if_no_outputs();

        auto input = get_next_input();
        if (input == inputs.end())
            return get_status_if_no_inputs();

        output->push(input->pull());
    }

    if (cur_output == outputs.end())
        return get_status_if_no_outputs();

    /// cur_input == inputs_end()
    return get_status_if_no_inputs();
}

}

