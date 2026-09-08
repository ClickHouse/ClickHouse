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
    , read_ahead_window(std::min(read_ahead_window_, num_lanes))
    /// Surviving rows a portion must yield before its lane counts as productive. The buffer
    /// budget is far larger than any single source block, so using it would activate
    /// speculation on every query; a fixed small batch keeps a dense start lazy even when
    /// `LIMIT` spans many blocks. Not an estimate of filter selectivity or source block size.
    , useful_rows_target(limit_ ? std::min<UInt64>(limit_, 1024) : 1024)
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
    candidates.reserve(num_lanes);
    ready_lanes.reserve(num_lanes);
}

void VirtualRowReadAheadTransform::finishLane(Lane & lane)
{
    lane.input->close();
    lane.output->finish();
    lane.chunks.clear();
    lane.boundary.clear();
    lane.finished = true;
    ++finished_lanes;
}

Columns VirtualRowReadAheadTransform::getBoundary(const Chunk & chunk, size_t row) const
{
    Columns boundary;
    boundary.reserve(sort_positions.size());
    for (size_t position : sort_positions)
        /// Index values and data can have different constant or sparse representations.
        /// Normalize only the copied key, leaving the original chunk untouched.
        boundary.push_back(chunk.getColumns()[position]->cut(row, 1)->convertToFullIfWrapped());
    return boundary;
}

int VirtualRowReadAheadTransform::compareBoundaries(const Columns & lhs, const Columns & rhs) const
{
    for (size_t i = 0; i < description.size(); ++i)
    {
        int order = description[i].direction
            * lhs[i]->compareAt(0, 0, *rhs[i], description[i].nulls_direction);
        if (order)
            return order;
    }
    return 0;
}

bool VirtualRowReadAheadTransform::earlier(size_t lhs, size_t rhs) const
{
    int order = compareBoundaries(lanes[lhs].boundary, lanes[rhs].boundary);
    return order ? order < 0 : lhs < rhs;
}

bool VirtualRowReadAheadTransform::canBuffer(const Lane & lane) const
{
    /// Match `BufferChunksTransform`: each queue fills until both thresholds are reached.
    /// The useful-output target controls activation, not the depth of an active buffer.
    return lane.buffered_rows < max_rows_to_buffer || lane.buffered_bytes < max_bytes_to_buffer;
}

bool VirtualRowReadAheadTransform::needsMoreSources(size_t lane_num, const Chunk & chunk) const
{
    size_t row = std::min<size_t>(chunk.getNumRows(), useful_rows_target) - 1;
    auto boundary = getBoundary(chunk, row);
    for (size_t i = 0; i < lanes.size(); ++i)
    {
        if (i == lane_num || lanes[i].input_finished || lanes[i].boundary.empty())
            continue;
        if (compareBoundaries(lanes[i].boundary, boundary) < 0)
            return true;
    }
    return false;
}

IProcessor::Status VirtualRowReadAheadTransform::prepare()
{
    candidates.clear();
    ready_lanes.clear();
    size_t occupied_slots = 0;

    for (size_t i = 0; i < lanes.size(); ++i)
    {
        auto & lane = lanes[i];
        lane.read_requested = false;
        if (lane.finished)
            continue;

        auto & input = *lane.input;
        auto & output = *lane.output;
        if (output.isFinished())
        {
            finishLane(lane);
            continue;
        }

        /// Initial announcements are requested from every lane. Only a later demand
        /// makes that lane active; it can then buffer even with a zero speculative window.
        if (lane.output_started && output.canPush())
            lane.demanded = true;

        /// An announcement always precedes the data read after it. As in
        /// `BufferChunksTransform`, the queue budget excludes the output port.
        if (output.canPush() && !lane.chunks.empty())
        {
            auto & next = lane.chunks.front();
            if (!isVirtualRow(next))
            {
                lane.buffered_rows -= next.getNumRows();
                lane.buffered_bytes -= next.bytes();
                lane.demanded = true;
            }
            output.push(std::move(next));
            lane.output_started = true;
            lane.chunks.pop_front();
        }

        if (lane.limit_reached)
            input.close();
        if (!lane.input_finished && input.isFinished())
        {
            lane.input_finished = true;
            if (!lane.boundary.empty() && lane.rows_since_boundary < useful_rows_target)
                read_ahead_started = true;
        }
        if (lane.input_finished)
        {
            /// Exhaustion releases the reader immediately, but buffered data must drain.
            if (lane.chunks.empty() && !output.hasData())
                finishLane(lane);
            continue;
        }

        /// Speculative data the merge has not demanded yet keeps its reader open, so the
        /// lane occupies a window slot until the merge reaches it.
        if (!lane.demanded && lane.buffered_rows)
            ++occupied_slots;

        /// Collect reads already in flight even if the lane has since left the window.
        if (input.hasData())
        {
            lane.incoming = input.pull(/* set_not_needed */ true);
            ready_lanes.push_back(i);
        }
        else if (output.canPush())
            lane.read_requested = true;
        else if (canBuffer(lane))
        {
            if (lane.demanded)
                lane.read_requested = true;
            else if (!lane.boundary.empty())
                candidates.push_back(i);
        }
    }

    /// Account for completed reads before granting more speculative work.
    if (ready_lanes.empty() && read_ahead_window && read_ahead_started)
    {
        /// Lanes already holding a slot may keep filling their buffers. Only the free
        /// slots go to lanes that have not read anything yet, earliest boundary first.
        auto fresh = std::partition(candidates.begin(), candidates.end(),
            [this](size_t lane_num)
            {
                return lanes[lane_num].buffered_rows > 0;
            });
        for (auto it = candidates.begin(); it != fresh; ++it)
            lanes[*it].read_requested = true;

        size_t free_slots = read_ahead_window > occupied_slots ? read_ahead_window - occupied_slots : 0;
        size_t count = std::min<size_t>(free_slots, candidates.end() - fresh);
        std::partial_sort(fresh, fresh + count, candidates.end(),
            [this](size_t lhs, size_t rhs)
            {
                return earlier(lhs, rhs);
            });
        for (auto it = fresh; it != fresh + count; ++it)
            lanes[*it].read_requested = true;
    }

    for (auto & lane : lanes)
    {
        if (lane.read_requested)
            lane.input->setNeeded();
        else
            lane.input->setNotNeeded();
    }

    if (!ready_lanes.empty())
        return Status::Ready;
    return finished_lanes == lanes.size() ? Status::Finished : Status::NeedData;
}

IProcessor::Status VirtualRowReadAheadTransform::prepare(const UpdatedInputPorts &, const UpdatedOutputPorts &)
{
    /// Selection can change permissions on ports that were not updated.
    return prepare();
}

void VirtualRowReadAheadTransform::work()
{
    for (size_t i : ready_lanes)
    {
        auto & lane = lanes[i];
        auto chunk = std::move(lane.incoming);
        if (isVirtualRow(chunk))
        {
            if (read_ahead_window)
            {
                if (!lane.boundary.empty() && lane.rows_since_boundary < useful_rows_target)
                    read_ahead_started = true;
                lane.rows_since_boundary = 0;

                /// Conversion mutates the metadata's block. Keep the original announcement
                /// intact because the merge must apply its own conversions exactly once.
                auto boundary = chunk.clone();
                setVirtualRow(boundary, *header, apply_virtual_row_conversions);
                lane.boundary = getBoundary(boundary, 0);
            }
            /// Only adjacent announcements can be replaced. Never skip across buffered data.
            if (!lane.chunks.empty() && isVirtualRow(lane.chunks.back()))
                lane.chunks.back() = std::move(chunk);
            else
                lane.chunks.push_back(std::move(chunk));
        }
        else
        {
            size_t rows = chunk.getNumRows();
            /// The merge skips empty real chunks. Do not accumulate them in its buffers.
            if (!rows)
            {
                read_ahead_started = true;
                continue;
            }
            if (read_ahead_window)
            {
                lane.rows_since_boundary += rows;
                if (rows < useful_rows_target)
                    read_ahead_started = true;
                /// Rows beyond another source's boundary cannot form a useful ordered batch
                /// until that source is read, even when filtering retained the whole chunk.
                if (!read_ahead_started && needsMoreSources(i, chunk))
                    read_ahead_started = true;
                lane.boundary = getBoundary(chunk, rows - 1);
            }
            /// Queued chunks outlive the reader; drop replicated rows nothing references so the
            /// queue holds, and `bytes()` accounts for, only what the merge will consume.
            compactReplicatedColumns(chunk);
            lane.buffered_rows += rows;
            lane.buffered_bytes += chunk.bytes();
            lane.chunks.push_back(std::move(chunk));

            /// As in `BufferChunksTransform`, a merge needs at most `LIMIT` rows per source.
            /// Ports are off limits in `work`; the input is closed by the next `prepare`.
            lane.rows_read += rows;
            if (limit && lane.rows_read >= limit)
                lane.limit_reached = true;
        }
    }
}

}
