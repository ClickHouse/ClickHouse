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

bool VirtualRowReadAheadTransform::accept(Lane & lane, const Chunk & chunk)
{
    bool is_virtual_row = isVirtualRow(chunk);
    if (lane.stage == Stage::Fresh)
    {
        if (is_virtual_row)
        {
            lane.stage = Stage::Deferred;
            lane.key = extractKey(chunk);
        }
        else
            lane.stage = Stage::Active;
    }

    if (is_virtual_row)
        return true;

    /// The merge skips empty chunks anyway; dropping them here spares it the round trip.
    if (!chunk.hasRows())
        return false;

    /// Real data: the reader is resident for the merge's sake, not the window's.
    lane.stage = Stage::Active;

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
    lane.stage = Stage::Finished;
}

void VirtualRowReadAheadTransform::wakeDeferredLanes(size_t prefetching)
{
    if (!deferred_order_built)
    {
        /// The order is by the announced keys, so it needs every lane's first chunk.
        for (const auto & lane : lanes)
            if (lane.stage == Stage::Fresh)
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
    while (next_deferred < deferred_order.size() && prefetching < read_ahead_window)
    {
        auto & lane = lanes[deferred_order[next_deferred++]];
        if (lane.stage != Stage::Deferred)
            continue;

        lane.stage = Stage::Prefetched;
        lane.parked = false;
        ++prefetching;
        if (!lane.input->isFinished())
            lane.input->setNeeded();
    }
}

IProcessor::Status VirtualRowReadAheadTransform::prepare()
{
    size_t finished = 0;
    size_t prefetching = 0;
    for (size_t i = 0; i < lanes.size(); ++i)
    {
        auto & lane = lanes[i];
        auto & input = *lane.input;
        auto & output = *lane.output;

        if (output.isFinished() && lane.stage != Stage::Finished)
            finishLane(lane);
        if (lane.stage == Stage::Finished)
        {
            ++finished;
            continue;
        }

        if (lane.parked && output.canPush())
        {
            /// The merge reached the announced key: the lane is active from here on.
            lane.parked = false;
            if (lane.stage != Stage::Active)
                lane.stage = Stage::Requested;
            if (last_lane_with_data >= 0 && last_lane_with_data != static_cast<ssize_t>(i))
                merge_advanced = true;
        }

        /// One chunk is pulled per pass: always when the merge is waiting for this lane with
        /// nothing queued, otherwise to read ahead into the buffer, but not while parked and not
        /// past an announcement the merge has not seen yet: it may park the lane there, and
        /// reading beyond would be the window's decision, not the buffer's.
        bool merge_waiting = output.canPush() && lane.chunks.empty();
        auto may_read_ahead = [&]
        {
            bool announcement_pending = !lane.chunks.empty() && isVirtualRow(lane.chunks.back());
            /// As in `BufferChunksTransform`: the queue fills until both thresholds are reached.
            bool below_caps = lane.buffered_rows < max_rows_to_buffer || lane.buffered_bytes < max_bytes_to_buffer;
            return !lane.parked && !announcement_pending && below_caps && !input.isFinished();
        };

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
                last_lane_with_data = static_cast<ssize_t>(i);
            }
            output.push(std::move(chunk));
        }

        if (input.isFinished() && lane.chunks.empty() && !output.hasData())
        {
            /// A lane the merge asked for ran dry without data: the merge has to move on to
            /// another lane, which is as good an advance as consuming one.
            if (lane.stage == Stage::Requested)
                merge_advanced = true;
            finishLane(lane);
            ++finished;
            continue;
        }

        if (lane.stage == Stage::Prefetched)
            ++prefetching;

        if (may_read_ahead())
            input.setNeeded();
        else
            input.setNotNeeded();
    }

    /// Without a `LIMIT` every deferred lane is reached eventually, so reading ahead only
    /// overlaps work. With one, wait for the merge to move past a lane: as long as it keeps
    /// asking the front lane for more, the lanes behind it may never be needed.
    if (read_ahead_window && (limit == 0 || merge_advanced))
        wakeDeferredLanes(prefetching);

    return finished == lanes.size() ? Status::Finished : Status::NeedData;
}

}
