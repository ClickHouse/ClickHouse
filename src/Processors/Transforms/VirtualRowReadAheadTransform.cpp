#include <Processors/Transforms/VirtualRowReadAheadTransform.h>

#include <Columns/IColumn.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>
#include <base/defines.h>

#include <algorithm>

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
    , num_fresh(num_lanes)
{
    auto input = inputs.begin();
    auto output = outputs.begin();
    for (size_t i = 0; i < num_lanes; ++i)
    {
        lanes[i].input = &*input++;
        lanes[i].output = &*output++;
        lane_of_port[lanes[i].input] = i;
        lane_of_port[lanes[i].output] = i;
    }
    for (const auto & column : description)
        sort_positions.push_back(header->getPositionByName(column.column_name));
}

Columns VirtualRowReadAheadTransform::extractKey(const Chunk & virtual_row) const
{
    /// Conversion mutates the metadata's block, and the merge must apply its own conversions
    /// exactly once, so work on a copy and leave the announcement itself untouched.
    auto converted = virtual_row.clone();
    setVirtualRow(converted, *header, apply_virtual_row_conversions);

    Columns key;
    key.reserve(sort_positions.size());
    for (size_t position : sort_positions)
        /// Index values and data can have different constant or sparse representations.
        key.push_back(converted.getColumns()[position]->cut(0, 1)->convertToFullIfWrapped());
    return key;
}

void VirtualRowReadAheadTransform::setStage(Lane & lane, Stage stage)
{
    /// A lane only moves forward: the first chunk decides between announcing and data,
    /// the window may wake a deferred lane, data makes a lane active, anything can finish.
    chassert(stage != lane.stage);
    switch (lane.stage)
    {
        case Stage::Fresh:
            chassert(stage == Stage::Deferred || stage == Stage::Active || stage == Stage::Finished);
            break;
        case Stage::Deferred:
            chassert(stage == Stage::Prefetched || stage == Stage::Active || stage == Stage::Finished);
            break;
        case Stage::Prefetched:
            chassert(stage == Stage::Active || stage == Stage::Finished);
            break;
        case Stage::Active:
            chassert(stage == Stage::Finished);
            break;
        case Stage::Finished:
            chassert(false);
            break;
    }

    if (lane.stage == Stage::Fresh)
        --num_fresh;
    if (lane.stage == Stage::Prefetched)
        --num_prefetching;
    if (stage == Stage::Prefetched)
        ++num_prefetching;
    if (stage == Stage::Finished)
        ++num_finished;
    lane.stage = stage;
}

bool VirtualRowReadAheadTransform::accept(Lane & lane, const Chunk & chunk)
{
    bool is_virtual_row = isVirtualRow(chunk);
    if (lane.stage == Stage::Fresh)
    {
        if (is_virtual_row)
        {
            setStage(lane, Stage::Deferred);
            lane.key = extractKey(chunk);
        }
        else
            setStage(lane, Stage::Active);
    }

    if (is_virtual_row)
        return true;

    /// The merge skips empty chunks anyway; dropping them here spares it the round trip.
    if (!chunk.hasRows())
        return false;

    /// As in `BufferChunksTransform`, a merge needs at most `LIMIT` rows per source.
    lane.rows_read += chunk.getNumRows();
    if (limit && lane.rows_read >= limit)
        lane.input->close();
    return true;
}

void VirtualRowReadAheadTransform::finishLane(Lane & lane)
{
    lane.input->close();
    lane.output->finish();
    lane.chunks.clear();
    setStage(lane, Stage::Finished);
}

void VirtualRowReadAheadTransform::wakeDeferredLanes()
{
    if (!deferred_order_built)
    {
        /// The order is by the announced keys, so it needs every lane's first chunk.
        if (num_fresh > 0)
            return;

        for (size_t i = 0; i < lanes.size(); ++i)
            if (!lanes[i].key.empty())
                deferred_order.push_back(i);
        std::sort(deferred_order.begin(), deferred_order.end(), [this](size_t lhs, size_t rhs)
        {
            const auto & left = lanes[lhs].key;
            const auto & right = lanes[rhs].key;
            for (size_t i = 0; i < description.size(); ++i)
                if (int order = description[i].direction * left[i]->compareAt(0, 0, *right[i], description[i].nulls_direction))
                    return order < 0;
            return lhs < rhs;
        });
        deferred_order_built = true;
    }

    /// Keep the next `read_ahead_window` deferred lanes reading, in the order the merge will
    /// need them; the lanes past the window wait until the merge comes closer, so the number
    /// of resident readers is bounded by the window rather than by the number of parts.
    while (next_deferred < deferred_order.size() && num_prefetching < read_ahead_window)
    {
        auto & lane = lanes[deferred_order[next_deferred++]];
        if (lane.stage != Stage::Deferred)
            continue;

        setStage(lane, Stage::Prefetched);
        lane.parked = false;
        if (!lane.input->isFinished())
            lane.input->setNeeded();
    }
}

void VirtualRowReadAheadTransform::processLane(size_t i, bool asked)
{
    auto & lane = lanes[i];
    auto & input = *lane.input;
    auto & output = *lane.output;

    if (lane.stage == Stage::Finished)
        return;
    if (output.isFinished())
    {
        finishLane(lane);
        return;
    }

    if (asked)
    {
        if (last_lane_with_data >= 0 && last_lane_with_data != static_cast<ssize_t>(i))
            merge_advanced = true;
        lane.parked = false;
    }

    /// Reading ahead of the merge's demand, but not while parked and not past an announcement
    /// the merge has not seen yet: one chunk is kept ready for the merge's input port, and
    /// behind it the buffer fills until both caps are reached, as in `BufferChunksTransform`.
    auto may_read_ahead = [&]
    {
        if (lane.parked || input.isFinished())
            return false;
        if (!lane.chunks.empty() && isVirtualRow(lane.chunks.back()))
            return false;
        if (lane.chunks.empty())
            return true;

        size_t rows = lane.buffered_rows;
        size_t bytes = lane.buffered_bytes;
        if (!output.hasData() && !isVirtualRow(lane.chunks.front()))
        {
            rows -= lane.chunks.front().getNumRows();
            bytes -= lane.chunks.front().bytes();
        }
        return rows < max_rows_to_buffer || bytes < max_bytes_to_buffer;
    };

    bool merge_waiting = output.canPush() && lane.chunks.empty();
    if (input.hasData() && (merge_waiting || may_read_ahead()))
    {
        Chunk chunk = input.pull(/* set_not_needed */ true);
        if (accept(lane, chunk))
        {
            if (isVirtualRow(chunk))
            {
                /// A later announcement only tightens an unconsumed one; keep the last.
                if (!lane.chunks.empty() && isVirtualRow(lane.chunks.back()))
                    lane.chunks.back() = std::move(chunk);
                else
                    lane.chunks.push_back(std::move(chunk));
            }
            else
            {
                /// A queued chunk outlives the reader; measure what the merge will consume.
                if (!merge_waiting)
                    compactReplicatedColumns(chunk);
                lane.buffered_rows += chunk.getNumRows();
                lane.buffered_bytes += chunk.bytes();
                lane.chunks.push_back(std::move(chunk));
            }
        }
    }

    if (output.canPush() && !lane.chunks.empty())
    {
        Chunk chunk = std::move(lane.chunks.front());
        lane.chunks.pop_front();
        if (isVirtualRow(chunk))
            lane.parked = true;
        else
        {
            lane.buffered_rows -= chunk.getNumRows();
            lane.buffered_bytes -= chunk.bytes();
            /// The merge takes a lane's first chunk while initializing, without consuming it,
            /// so a lane that started with data enters the bookkeeping from its second chunk.
            if (lane.delivered_data || !lane.key.empty())
                last_lane_with_data = static_cast<ssize_t>(i);
            lane.delivered_data = true;
            if (lane.stage != Stage::Active)
                setStage(lane, Stage::Active);
        }
        output.push(std::move(chunk));
    }

    if (input.isFinished() && lane.chunks.empty() && !output.hasData())
    {
        /// An exhausted lane is closed when the merge asks for it, not before: a woken lane
        /// keeps its window slot until then, and the merge having to move on from a lane it
        /// asked for is what justifies reading ahead under a `LIMIT`. The window is not
        /// refilled on this request, though: the merge may finish once it learns the lane
        /// is exhausted, and otherwise its next request refills it.
        if (!output.canPush())
            return;
        merge_advanced = true;
        finishLane(lane);
        return;
    }

    if ((output.canPush() && lane.chunks.empty()) || may_read_ahead())
        input.setNeeded();
    else
        input.setNotNeeded();
}

IProcessor::Status VirtualRowReadAheadTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    ++pass;
    bool merge_asked = false;
    for (const auto * port : updated_outputs)
    {
        size_t i = lane_of_port.at(port);
        if (lanes[i].visited_pass == pass)
            continue;
        lanes[i].visited_pass = pass;
        bool asked = port->canPush();
        processLane(i, asked);
        if (asked && lanes[i].stage != Stage::Finished)
            merge_asked = true;
    }
    for (const auto * port : updated_inputs)
    {
        size_t i = lane_of_port.at(port);
        if (lanes[i].visited_pass == pass)
            continue;
        lanes[i].visited_pass = pass;
        processLane(i, /* asked */ false);
    }

    /// Without a `LIMIT` every deferred lane is reached eventually, so reading ahead only
    /// overlaps work. With one, wait for the merge to move past a lane: as long as it keeps
    /// asking the front lane for more, the lanes behind it may never be needed.
    if (merge_asked && read_ahead_window && (limit == 0 || merge_advanced))
        wakeDeferredLanes();

    return num_finished == lanes.size() ? Status::Finished : Status::NeedData;
}

IProcessor::Status VirtualRowReadAheadTransform::prepare()
{
    UpdatedInputPorts all_inputs;
    UpdatedOutputPorts all_outputs;
    for (auto & lane : lanes)
    {
        all_inputs.push_back(lane.input);
        all_outputs.push_back(lane.output);
    }
    return prepare(all_inputs, all_outputs);
}

}
