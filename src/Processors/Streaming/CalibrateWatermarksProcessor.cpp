#include <Processors/Streaming/CalibrateWatermarksProcessor.h>
#include <Processors/Streaming/Markers.h>

#include <Processors/Port.h>

#include <Common/logger_useful.h>

#include <Core/Block.h>

#include <base/defines.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

void drainQueue(OutputPort * output, std::deque<Chunk> & queue)
{
    if (!queue.empty() && output->canPush())
    {
        output->push(std::move(queue.front()));
        queue.pop_front();
    }
}

}

void CalibrateWatermarksProcessor::handleOutputUpdate(OutputPort * output, OutputState & state)
{
    if (output->isFinished())
    {
        state.queue.clear();
        finished_outputs.insert(output);
        return;
    }

    drainQueue(output, state.queue);
}

void CalibrateWatermarksProcessor::handleInputUpdate(InputPort * input, InputState & input_state)
{
    if (input->isFinished())
    {
        if (finished_inputs.insert(input).second)
            mergeInputStates();

        return;
    }

    if (!input->hasData())
        return;

    Chunk chunk = input->pull();
    input->setNeeded();

    if (chunk.getChunkInfos().has<IdleMarker>())
    {
        input_state.idle = true;
        mergeInputStates();
    }
    else if (auto marker = chunk.getChunkInfos().get<WatermarkMarker>())
    {
        input_state.idle = false;

        if (marker->watermark > input_state.watermark)
            input_state.watermark = marker->watermark;

        mergeInputStates();
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

size_t CalibrateWatermarksProcessor::getPendingQueuesCount() const
{
    size_t count = 0;

    for (const auto & [output, output_state] : outputs_state)
        if (!output_state.queue.empty())
            count += 1;

    return count;
}

void CalibrateWatermarksProcessor::broadcastMarker(const Chunk & marker)
{
    for (auto & [output, output_state] : outputs_state)
    {
        if (output->isFinished())
            continue;

        output_state.queue.push_back(marker.clone());
        drainQueue(output, output_state.queue);
    }
}

void CalibrateWatermarksProcessor::mergeInputStates()
{
    /// All inputs finished is pipeline shutdown, not idleness.
    if (finished_inputs.size() == inputs_state.size())
        return;

    Field min_watermark;
    bool all_idle = true;
    for (const auto & [input, input_state] : inputs_state)
    {
        if (input_state.idle)
            continue;

        if (all_idle || input_state.watermark < min_watermark)
            min_watermark = input_state.watermark;

        all_idle = false;
    }

    if (all_idle)
    {
        if (!std::exchange(emitted_idle, true))
        {
            LOG_TEST(log, "Broadcasting idle marker");
            broadcastMarker(IdleMarker::create(outputs.front().getHeader()));
        }
    }
    else if (min_watermark > last_emitted_watermark)
    {
        last_emitted_watermark = min_watermark;
        emitted_idle = false;
        LOG_TEST(log, "Broadcasting watermark: {}", min_watermark);
        broadcastMarker(WatermarkMarker::create(outputs.front().getHeader(), min_watermark));
    }
}

CalibrateWatermarksProcessor::CalibrateWatermarksProcessor(SharedHeader header, size_t num_inputs, size_t num_outputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
    , log(getLogger("CalibrateWatermarks"))
{
    if (num_inputs == 0 || num_outputs == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CalibrateWatermarksProcessor requires at least one input and one output");

    for (auto & port : inputs)
        inputs_state[&port] = InputState{};

    for (auto & port : outputs)
        outputs_state[&port] = OutputState{};
}

IProcessor::Status CalibrateWatermarksProcessor::prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & updated_output_ports)
{
    if (std::exchange(initialized, true) == false)
        for (auto & port : inputs)
            port.setNeeded();

    for (auto * input : updated_input_ports)
        handleInputUpdate(input, inputs_state.at(input));

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
