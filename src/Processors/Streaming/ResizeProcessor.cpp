#include <Processors/Streaming/ResizeProcessor.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{
ShrinkResizeProcessor::ShrinkResizeProcessor(const Block & header, size_t num_inputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(1, header))
    , log(&Poco::Logger::get("ShrinkResizeProcessor"))
{
    assert(num_inputs > 0);
}

IProcessor::Status ShrinkResizeProcessor::prepare(const PortNumbers & updated_inputs, const PortNumbers & /*updated_outputs*/)
{
    if (unlikely(!initialized))
    {
        initialized = true;

        for (auto & input : inputs)
        {
            assert(input.getOutputPort().getProcessor().isStreaming());
            input.setNeeded();
            input_ports.push_back({.port = &input, .status = InputStatus::NeedData, .watermark = INVALID_WATERMARK});
        }
    }

    /// Update inputs
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

    if (num_finished_inputs == inputs.size())
    {
        for (auto output : outputs)
            output.finish();

        return Status::Finished;
    }

    /// Check output can push
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    /// Check inputs has data
    for (auto & input_port : input_ports)
    {
        if (input_port.status == InputStatus::NeedData)
            input_port.port->setNeeded();
    }

    if (!inputs_with_data.empty())
    {
        auto & input_with_data = input_ports[inputs_with_data.front()];
        inputs_with_data.pop();

        auto data = input_with_data.port->pullData(true);
        if (updateAndAlignWatermark(input_with_data, data.chunk))
        {
            /// Do nothing
        }
        else
            input_with_data.status = InputStatus::NeedData;

        output.pushData(std::move(data));
        return Status::PortFull;
    }

    return Status::NeedData;
}

bool ShrinkResizeProcessor::updateAndAlignWatermark(InputPortWithStatus & input_with_data, Chunk & chunk)
{
    if (!chunk.hasWatermark())
        return false;

    bool updated = false;
    auto new_watermark = chunk.getWatermark();
    if (new_watermark > input_with_data.watermark || (input_with_data.watermark == TIMEOUT_WATERMARK && new_watermark >= aligned_watermark))
    {
        input_with_data.watermark = new_watermark;
        auto min_watermark
            = std::ranges::min(input_ports, [](const auto & l, const auto & r) { return l.watermark < r.watermark; }).watermark;
        if (min_watermark > aligned_watermark)
        {
            aligned_watermark = min_watermark;
            updated = true;
        }
    }
    else
    {
        if (unlikely(new_watermark < aligned_watermark))
            LOG_INFO(log, "Found outdate watermark. aligned watermark={}, but got watermark = {}", aligned_watermark, new_watermark);
    }

    input_with_data.status = InputStatus::NeedData;

    if (updated)
        chunk.getChunkContext()->setWatermark(aligned_watermark);
    else
        chunk.clearWatermark();

    return true;
}

ExpandResizeProcessor::ExpandResizeProcessor(const Block & header, size_t num_outputs)
    : IProcessor(InputPorts(1, header), OutputPorts(num_outputs, header))
    , header_chunk(outputs.front().getHeader().getColumns(), 0)
{
    assert(num_outputs > 0);
}

IProcessor::Status ExpandResizeProcessor::prepare(const PortNumbers & /*updated_inputs*/, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;

        for (auto & input : inputs)
            input.setNeeded();

        for (auto & output : outputs)
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
    }

    /// Update outputs
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
                waiting_outputs.push_back(&output);
            }
        }
    }

    if (num_finished_outputs == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    /// Check input is finished
    auto & input = inputs.front();
    if (input.isFinished())
    {
        /// Flush all outputs before finish
        bool all_outputs_finished = true;
        for (auto & output : output_ports)
        {
            if (output.port->isFinished())
                continue;

            if (output.port->hasData() || output.propagate_flag)
            {
                all_outputs_finished = false;
                continue;
            }

            output.port->finish();
        }

        if (all_outputs_finished)
            return Status::Finished;
    }

    /// Check input has data
    if (!waiting_outputs.empty())
    {
        input.setNeeded();

        if (input.hasData())
        {
            auto data = input.pullData(/*set_not_needed*/ true);
            if (data.chunk.hasWatermark())
            {
                std::ranges::for_each(
                    output_ports, [](auto & output) { output.propagate_flag |= OutputPortWithStatus::PROPAGATE_WATERMARK; });
                watermark = std::max(watermark, data.chunk.getWatermark());
            }
            else if (!data.chunk.hasRows())
            {
                std::ranges::for_each(
                    output_ports, [](auto & output) { output.propagate_flag |= OutputPortWithStatus::PROPAGATE_HEARTBEAT; });
            }
            
            if (data.chunk.hasRows())
            {
                auto & waiting_output = *waiting_outputs.front();
                waiting_outputs.pop_front();
                waiting_output.port->pushData(std::move(data));
                waiting_output.propagate_flag = OutputPortWithStatus::NO_PROPAGATE;
                waiting_output.status = OutputStatus::NotActive;
            }
        }
    }

    /// Try propagate some context (e.g. watermark/checkpoint or heartbeat)
    for (auto iter = waiting_outputs.begin(); iter != waiting_outputs.end();)
    {
        auto & waiting_output = **iter;
        if (waiting_output.propagate_flag)
        {
            auto chunk = header_chunk.clone();

            if (waiting_output.propagate_flag & OutputPortWithStatus::PROPAGATE_WATERMARK)
            {
                chunk.getOrCreateChunkContext()->setWatermark(watermark);
                waiting_output.propagate_flag &= ~(OutputPortWithStatus::PROPAGATE_WATERMARK | OutputPortWithStatus::PROPAGATE_HEARTBEAT);
            }
            else
            {
                waiting_output.propagate_flag &= ~OutputPortWithStatus::PROPAGATE_HEARTBEAT;
            }

            waiting_output.port->push(std::move(chunk));
            waiting_output.status = OutputStatus::NotActive;
            iter = waiting_outputs.erase(iter);
        }
        else
            ++iter;
    }

    if (!waiting_outputs.empty())
        return Status::NeedData;

    return Status::PortFull;
}

StrictResizeProcessor::StrictResizeProcessor(const Block & header, size_t num_inputs_and_outputs)
    : IProcessor(
        InputPorts(num_inputs_and_outputs, header),
        OutputPorts(num_inputs_and_outputs, header))
{
    assert(num_inputs_and_outputs > 0);
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No associated output for input with data.");

        auto & waiting_output = output_ports[input_with_data.waiting_output];

        if (waiting_output.status == OutputStatus::NotActive)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid status NotActive for associated output.");

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
}
