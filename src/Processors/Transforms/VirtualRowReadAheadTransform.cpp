#include <Processors/Transforms/VirtualRowReadAheadTransform.h>

#include <Processors/Port.h>

namespace DB
{

VirtualRowReadAheadTransform::VirtualRowReadAheadTransform(
    SharedHeader header_,
    size_t num_lanes,
    SortDescription description_,
    bool apply_virtual_row_conversions_,
    UInt64 limit_,
    size_t max_rows_to_buffer_,
    size_t max_bytes_to_buffer_,
    size_t read_ahead_window_)
    : IProcessor(InputPorts(num_lanes, header_), OutputPorts(num_lanes, header_))
    , header(std::move(header_))
    , description(std::move(description_))
    , apply_virtual_row_conversions(apply_virtual_row_conversions_)
    , limit(limit_)
    , max_rows_to_buffer(max_rows_to_buffer_)
    , max_bytes_to_buffer(max_bytes_to_buffer_)
    , read_ahead_window(read_ahead_window_)
    , lanes(num_lanes)
{
    size_t lane_num = 0;
    auto input = inputs.begin();
    auto output = outputs.begin();
    for (; input != inputs.end(); ++input, ++output, ++lane_num)
    {
        lanes[lane_num].input = &*input;
        lanes[lane_num].output = &*output;
        lane_by_port[&*input] = lane_num;
        lane_by_port[&*output] = lane_num;
    }
}

bool VirtualRowReadAheadTransform::mayRead(size_t /* lane_num */) const
{
    return true;
}

void VirtualRowReadAheadTransform::consume(size_t lane_num, Chunk chunk)
{
    lanes[lane_num].output->push(std::move(chunk));
}

void VirtualRowReadAheadTransform::finishLane(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    lane.finished = true;
    ++finished_lanes;
    lane.output->finish();
    lane.input->close();
}

void VirtualRowReadAheadTransform::serve(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.finished)
        return;

    if (lane.output->isFinished() || lane.input->isFinished())
    {
        finishLane(lane_num);
        return;
    }

    if (!lane.output->canPush() || !mayRead(lane_num))
    {
        lane.input->setNotNeeded();
        return;
    }

    lane.input->setNeeded();
    if (lane.input->hasData())
        consume(lane_num, lane.input->pull(/* set_not_needed */ true));
}

IProcessor::Status VirtualRowReadAheadTransform::prepare()
{
    UpdatedInputPorts all_inputs;
    UpdatedOutputPorts all_outputs;
    all_inputs.reserve(inputs.size());
    all_outputs.reserve(outputs.size());
    for (auto & input : inputs)
        all_inputs.push_back(&input);
    for (auto & output : outputs)
        all_outputs.push_back(&output);
    return prepareImpl(all_inputs, all_outputs);
}

IProcessor::Status VirtualRowReadAheadTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    return prepareImpl(updated_inputs, updated_outputs);
}

IProcessor::Status VirtualRowReadAheadTransform::prepareImpl(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;
        for (size_t lane_num = 0; lane_num < lanes.size(); ++lane_num)
            serve(lane_num);
    }
    for (const auto * output : updated_outputs)
        serve(lane_by_port.at(output));
    for (const auto * input : updated_inputs)
        serve(lane_by_port.at(input));

    if (finished_lanes == lanes.size())
        return Status::Finished;
    return Status::NeedData;
}

}
