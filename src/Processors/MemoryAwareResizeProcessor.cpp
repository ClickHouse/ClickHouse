#include <Processors/MemoryAwareResizeProcessor.h>
#include <Common/logger_useful.h>

namespace DB
{

MemoryAwareResizeProcessor::MemoryAwareResizeProcessor(
    SharedHeader header,
    size_t num_inputs,
    size_t num_outputs,
    InsertMemoryThrottlePtr throttle_)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
    , throttle(std::move(throttle_))
    , allowed_outputs(num_outputs)
{
}

void MemoryAwareResizeProcessor::updateAllowedOutputs()
{
    if (!throttle)
        return;

    const size_t total = output_ports.empty() ? allowed_outputs : output_ports.size();
    const size_t prev_allowed = allowed_outputs;
    allowed_outputs = throttle->calculateAllowedOutputs(total, min_outputs);

    /// Outputs we just re-enabled won't be re-notified by the executor, wake them up
    if (allowed_outputs > prev_allowed && initialized)
    {
        for (size_t i = prev_allowed; i < allowed_outputs && i < output_ports.size(); ++i)
        {
            auto & output = output_ports[i];
            if (output.port->isFinished())
            {
                if (output.status != OutputStatus::Finished)
                {
                    ++num_finished_outputs;
                    output.status = OutputStatus::Finished;
                }
                continue;
            }
            if (output.status != OutputStatus::NeedData && output.port->canPush())
            {
                output.status = OutputStatus::NeedData;
                waiting_outputs.push(i);
            }
        }
    }
}

IProcessor::Status MemoryAwareResizeProcessor::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input_ports.push_back({.port = &input, .status = InputStatus::NotActive});

        for (auto & output : outputs)
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
    }

    updateAllowedOutputs();

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

        if (output_number < allowed_outputs && output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;
                waiting_outputs.push(output_number);
            }
        }
    }

    /// if every active output is finished but inactive ones remain,
    /// expand `allowed_outputs` so the pipeline can drain
    if (allowed_outputs < output_ports.size())
    {
        bool any_active_alive = false;
        for (size_t i = 0; i < allowed_outputs && i < output_ports.size(); ++i)
        {
            if (output_ports[i].status != OutputStatus::Finished)
            {
                any_active_alive = true;
                break;
            }
        }
        if (!any_active_alive)
        {
            for (size_t i = allowed_outputs; i < output_ports.size(); ++i)
            {
                auto & output = output_ports[i];
                if (output.port->isFinished())
                {
                    if (output.status != OutputStatus::Finished)
                    {
                        ++num_finished_outputs;
                        output.status = OutputStatus::Finished;
                    }
                    continue;
                }
                if (output.status != OutputStatus::NeedData && output.port->canPush())
                {
                    output.status = OutputStatus::NeedData;
                    waiting_outputs.push(i);
                }
            }
            allowed_outputs = output_ports.size();
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
        auto output_idx = waiting_outputs.front();
        waiting_outputs.pop();

        if (output_idx >= allowed_outputs)
        {
            output_ports[output_idx].status = OutputStatus::NotActive;
            continue;
        }

        auto & waiting_output = output_ports[output_idx];

        if (waiting_output.port->isFinished())
        {
            if (waiting_output.status != OutputStatus::Finished)
            {
                ++num_finished_outputs;
                waiting_output.status = OutputStatus::Finished;
            }
            continue;
        }

        auto & input_with_data = input_ports[inputs_with_data.front()];
        inputs_with_data.pop();

        auto data = input_with_data.port->pullData();
        if (throttle)
            throttle->observeChunkBytes(data.chunk.bytes());
        waiting_output.port->pushData(std::move(data));
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

}
