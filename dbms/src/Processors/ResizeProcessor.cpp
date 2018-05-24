#include <Processors/ResizeProcessor.h>


namespace DB
{

ResizeProcessor::Status ResizeProcessor::prepare()
{
    while (true)
    {
        bool all_outputs_full = true;
        bool all_outputs_unneeded = true;

        for (const auto & output : outputs)
        {
            if (!output.hasData())
                all_outputs_full = false;

            if (output.isNeeded())
                all_outputs_unneeded = false;
        }

        if (all_outputs_full)
            return Status::PortFull;

        if (all_outputs_unneeded)
        {
            for (auto & input : inputs)
                input.setNotNeeded();

            return Status::Unneeded;
        }

        bool all_inputs_finished = true;
        bool all_inputs_have_no_data = true;

        for (auto & input : inputs)
        {
            if (!input.isFinished())
            {
                all_inputs_finished = false;

                input.setNeeded();
                if (input.hasData())
                    all_inputs_have_no_data = false;
            }
        }

        if (all_inputs_have_no_data)
        {
            if (all_inputs_finished)
                return Status::Finished;
            else
                return Status::NeedData;
        }

        for (auto & input : inputs)
        {
            if (input.hasData())
            {
                for (auto & output : outputs)
                {
                    if (!output.hasData())
                    {
                        output.push(input.pull());
                        break;
                    }
                }
                break;
            }
        }
    }
}

}

