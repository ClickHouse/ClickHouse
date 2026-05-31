#include <Processors/IProcessor.h>
#include <Processors/Streaming/WatermarkMerger.h>
#include <Processors/Streaming/MarkerWatermark.h>
#include <Processors/Streaming/MarkerIdle.h>

#include <Columns/IColumn.h>

#include <Processors/Port.h>

#include <Common/logger_useful.h>

#include <Core/Block.h>

#include <cstdint>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

bool isMarkerChunk(const Chunk & chunk)
{
    return chunk.getNumRows() == 0
        && (chunk.getChunkInfos().has<WatermarkMarker>() || chunk.getChunkInfos().has<IdleMarker>());
}

void enqueueMarker(std::deque<Chunk> & queue, Chunk marker)
{
    if (!queue.empty() && isMarkerChunk(queue.back()))
        queue.back() = std::move(marker);
    else
        queue.push_back(std::move(marker));
}

void drainQueue(OutputPort * output, std::deque<Chunk> & queue)
{
    if (!queue.empty() && output->canPush())
    {
        output->push(std::move(queue.front()));
        queue.pop_front();
    }
}

}

WatermarkMerger::WatermarkMerger(SharedHeader header, size_t num_inputs, size_t num_outputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
    , log(getLogger("WatermarkMerger"))
{
    if (num_inputs == 0 || num_outputs == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WatermarkMerger requires at least one input and one output");

    for (auto & port : inputs)
        inputs_state[&port] = InputState{};

    for (auto & port : outputs)
        outputs_state[&port] = OutputState{};
}

void WatermarkMerger::handleOutputUpdate(OutputPort * output, OutputState & state)
{
    if (output->isFinished())
    {
        state.queue.clear();
        finished_outputs.insert(output);
        return;
    }

    drainQueue(output, state.queue);
}

void WatermarkMerger::handleInputUpdate(InputPort * input, InputState & input_state)
{
    if (input->isFinished())
    {
        input_state.idle = true;
        finished_inputs.insert(input);
        marked_inputs.insert(input);
        marked_state_reported = false;
    }

    if (!input->hasData())
        return;

    Chunk chunk = input->pull();
    input->setNeeded();

    if (chunk.getChunkInfos().has<IdleMarker>())
    {
        input_state.idle = true;
        marked_inputs.insert(input);
        marked_state_reported = false;
    }
    else if (chunk.getChunkInfos().has<WatermarkMarker>())
    {
        if (!input->isFinished())
            input_state.idle = false;

        input_state.pending_watermark = chunk.getChunkInfos().get<WatermarkMarker>()->watermark;
        marked_inputs.insert(input);
        marked_state_reported = false;
    }
    else
    {
        OutputPort * picked_output = nullptr;
        OutputState * picked_output_state = nullptr;
        for (auto & [output, output_state] : outputs_state)
        {
            if (output->isFinished())
                continue;

            if (!picked_output_state || output_state.queue.size() < picked_output_state->queue.size())
            {
                picked_output = output;
                picked_output_state = &output_state;
            }
        }

        if (!picked_output)
            return;

        picked_output_state->queue.push_back(std::move(chunk));
        drainQueue(picked_output, picked_output_state->queue);
    }
}

void WatermarkMerger::broadcastAlignedMarker()
{
    uint64_t num_idle = 0;
    std::optional<Field> min_watermark;
    for (const auto & [input, input_state] : inputs_state)
    {
        if (input_state.pending_watermark)
        {
            if (!min_watermark || *min_watermark > *input_state.pending_watermark)
                min_watermark = input_state.pending_watermark;
        }

        else if (input_state.idle)
            num_idle += 1;
    }

    if (num_idle == inputs.size())
    {
        LOG_TEST(log, "Broadcasting idle marker");
        for (auto & [output, output_state] : outputs_state)
        {
            if (output->isFinished())
                continue;

            enqueueMarker(output_state.queue, makeIdleMarkerChunk(output->getHeader()));
            drainQueue(output, output_state.queue);
        }
    }
    else
    {
        for (auto & [output, output_state] : outputs_state)
        {
            if (output->isFinished())
                continue;

            enqueueMarker(output_state.queue, makeWatermarkMarkerChunk(output->getHeader(), *min_watermark));
            drainQueue(output, output_state.queue);
        }

        LOG_TEST(log, "Broadcasting watermark: {}", min_watermark.value());
        for (auto & [input, input_state] : inputs_state)
        {
            if (!input_state.pending_watermark)
                continue;

            if (*input_state.pending_watermark > *min_watermark)
                continue;

            input_state.pending_watermark.reset();

            if (!input_state.idle)
                marked_inputs.erase(input);
        }
    }

    marked_state_reported = true;
}

size_t WatermarkMerger::getPendingQueuesCount() const
{
    size_t count = 0;

    for (const auto & [output, output_state] : outputs_state)
        if (!output_state.queue.empty())
            count += 1;

    return count;
}

IProcessor::Status WatermarkMerger::prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & updated_output_ports)
{
    if (std::exchange(initialized, true) == false)
        for (auto & port : inputs)
            port.setNeeded();

    for (auto * input : updated_input_ports)
        handleInputUpdate(input, inputs_state.at(input));

    /// Special case - if all input ports marked - broadcast markers.
    if (marked_inputs.size() == inputs.size())
        if (!marked_state_reported)
            broadcastAlignedMarker();

    for (auto * output : updated_output_ports)
        handleOutputUpdate(output, outputs_state.at(output));

    if (finished_outputs.size() == outputs.size())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (finished_inputs.size() == inputs.size())
    {
        if (getPendingQueuesCount() > 0)
            return Status::PortFull;

        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    if (getPendingQueuesCount() == outputs.size() - finished_outputs.size())
        return Status::PortFull;

    return IProcessor::Status::NeedData;
}

}
