#include <Processors/Transforms/VirtualRowReadAheadTransform.h>

#include <Columns/IColumn.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

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
{
    auto input = inputs.begin();
    auto output = outputs.begin();
    for (auto & lane : lanes)
    {
        lane.input = &*input++;
        lane.output = &*output++;
    }
    for (const auto & column : description)
        sort_positions.push_back(header->getPositionByName(column.column_name));
}

void VirtualRowReadAheadTransform::finishLane(Lane & lane)
{
    lane.input->close();
    lane.output->finish();
    lane.chunks.clear();
    lane.finished = true;
    ++finished_lanes;
}

void VirtualRowReadAheadTransform::releaseSlot(Lane & lane)
{
    lane.deferred = false;
    if (lane.prefetched)
    {
        lane.prefetched = false;
        --prefetches_in_flight;
    }
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

bool VirtualRowReadAheadTransform::keyLess(size_t lhs, size_t rhs) const
{
    const auto & left = lanes[lhs].key;
    const auto & right = lanes[rhs].key;
    for (size_t i = 0; i < description.size(); ++i)
    {
        int order = description[i].direction * left[i]->compareAt(0, 0, *right[i], description[i].nulls_direction);
        if (order)
            return order < 0;
    }
    return lhs < rhs;
}

bool VirtualRowReadAheadTransform::canBuffer(const Lane & lane) const
{
    /// As in `BufferChunksTransform`: the queue fills until both thresholds are reached.
    return lane.buffered_rows < max_rows_to_buffer || lane.buffered_bytes < max_bytes_to_buffer;
}

bool VirtualRowReadAheadTransform::accept(Lane & lane, const Chunk & chunk)
{
    bool is_virtual_row = isVirtualRow(chunk);
    if (!lane.started)
    {
        lane.started = true;
        if (is_virtual_row)
        {
            lane.deferred = true;
            lane.key = extractKey(chunk);
        }
    }

    if (is_virtual_row)
        return true;

    /// The merge skips empty chunks anyway; dropping them here spares it the round trip.
    if (!chunk.hasRows())
        return false;

    /// Real data: the reader is resident for the merge's sake, not the window's.
    lane.had_data = true;
    releaseSlot(lane);

    /// As in `BufferChunksTransform`, a merge needs at most `LIMIT` rows per source.
    lane.rows_read += chunk.getNumRows();
    if (limit && lane.rows_read >= limit)
        lane.input->close();
    return true;
}

void VirtualRowReadAheadTransform::deliver(size_t lane_num, Chunk chunk)
{
    auto & lane = lanes[lane_num];
    if (isVirtualRow(chunk))
        lane.parked = true;
    else
        last_lane_with_data = static_cast<ssize_t>(lane_num);
    lane.output->push(std::move(chunk));
}

void VirtualRowReadAheadTransform::wakeDeferredLanes()
{
    if (!deferred_order_built)
    {
        /// The order is by the announced keys, so it needs every lane's first chunk.
        for (const auto & lane : lanes)
            if (!lane.started && !lane.finished)
                return;

        for (size_t i = 0; i < lanes.size(); ++i)
            if (!lanes[i].key.empty())
                deferred_order.push_back(i);
        std::sort(deferred_order.begin(), deferred_order.end(), [this](size_t lhs, size_t rhs) { return keyLess(lhs, rhs); });
        deferred_order_built = true;
    }

    /// Keep the next `read_ahead_window` deferred lanes reading, in the order the merge will
    /// need them; the lanes past the window wait until the merge comes closer, so the number
    /// of resident readers is bounded by the window rather than by the number of parts.
    while (next_deferred < deferred_order.size() && prefetches_in_flight < read_ahead_window)
    {
        auto & lane = lanes[deferred_order[next_deferred++]];
        if (!lane.deferred || lane.finished)
            continue;

        lane.deferred = false;
        lane.prefetched = true;
        lane.parked = false;
        ++prefetches_in_flight;
        if (!lane.input->isFinished())
            lane.input->setNeeded();
    }
}

IProcessor::Status VirtualRowReadAheadTransform::prepare()
{
    for (size_t i = 0; i < lanes.size(); ++i)
    {
        auto & lane = lanes[i];
        if (lane.finished)
            continue;

        auto & input = *lane.input;
        auto & output = *lane.output;
        if (output.isFinished())
        {
            releaseSlot(lane);
            finishLane(lane);
            continue;
        }

        if (output.canPush())
        {
            if (lane.parked)
            {
                /// The merge reached the announced key: the lane is active from here on.
                lane.parked = false;
                lane.demanded = true;
                releaseSlot(lane);
                if (last_lane_with_data >= 0 && last_lane_with_data != static_cast<ssize_t>(i))
                    merge_advanced = true;
            }

            if (!lane.chunks.empty())
            {
                Chunk chunk = std::move(lane.chunks.front());
                lane.chunks.pop_front();
                if (!isVirtualRow(chunk))
                {
                    lane.buffered_rows -= chunk.getNumRows();
                    lane.buffered_bytes -= chunk.bytes();
                }
                deliver(i, std::move(chunk));
            }
            else if (input.hasData())
            {
                Chunk chunk = input.pull(/* set_not_needed */ true);
                if (accept(lane, chunk))
                    deliver(i, std::move(chunk));
            }
        }

        if (input.isFinished() && lane.chunks.empty() && !output.hasData())
        {
            releaseSlot(lane);
            /// A lane the merge asked for ran dry without data: the merge has to move on to
            /// another lane, which is as good an advance as consuming one.
            if (lane.demanded && !lane.had_data)
                merge_advanced = true;
            finishLane(lane);
            continue;
        }

        /// Read ahead into the buffer, but not while parked and not past an announcement the
        /// merge has not seen yet: it may park the lane there, and reading beyond would be
        /// the window's decision, not the buffer's.
        auto may_read = [&]
        {
            bool announcement_pending = !lane.chunks.empty() && isVirtualRow(lane.chunks.back());
            return !lane.parked && !announcement_pending && canBuffer(lane) && !input.isFinished();
        };

        if (may_read() && input.hasData())
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
                    /// Queued chunks outlive the reader; measure what the merge will consume.
                    compactReplicatedColumns(chunk);
                    lane.buffered_rows += chunk.getNumRows();
                    lane.buffered_bytes += chunk.bytes();
                    lane.chunks.push_back(std::move(chunk));
                }
            }
        }

        if (may_read())
            input.setNeeded();
        else
            input.setNotNeeded();
    }

    /// Without a `LIMIT` every deferred lane is reached eventually, so reading ahead only
    /// overlaps work. With one, wait for the merge to move past a lane: as long as it keeps
    /// asking the front lane for more, the lanes behind it may never be needed.
    if (read_ahead_window && (limit == 0 || merge_advanced))
        wakeDeferredLanes();

    return finished_lanes == lanes.size() ? Status::Finished : Status::NeedData;
}

}
