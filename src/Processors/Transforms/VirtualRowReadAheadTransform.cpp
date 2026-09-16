#include <Processors/Transforms/VirtualRowReadAheadTransform.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
    /// A limit may be met by the first lane read, so wider speculation waits for evidence of sparsity.
    speculation_allowance = limit ? std::min<size_t>(read_ahead_window, 1) : read_ahead_window;
    candidates.reserve(num_lanes);
    data_lanes.reserve(num_lanes);
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

Columns VirtualRowReadAheadTransform::getBoundary(const Columns & key_columns, size_t row) const
{
    Columns boundary;
    boundary.reserve(key_columns.size());
    for (const auto & column : key_columns)
        /// Index values and data can have different constant or sparse representations.
        /// Normalize only the copied key, leaving the original chunk untouched.
        boundary.push_back(column->cut(row, 1)->convertToFullIfWrapped());
    return boundary;
}

Columns VirtualRowReadAheadTransform::getBoundary(const Chunk & chunk, size_t row) const
{
    Columns key_columns;
    key_columns.reserve(sort_positions.size());
    for (size_t position : sort_positions)
        key_columns.push_back(chunk.getColumns()[position]);
    return getBoundary(key_columns, row);
}

Columns VirtualRowReadAheadTransform::heldRowKey(const Lane & lane, size_t row) const
{
    /// Held rows in key order: the chunk the merge is consuming, the one in the port, the queue.
    if (row < lane.taken_rows)
        return getBoundary(lane.taken_keys, row);
    row -= lane.taken_rows;
    if (row < lane.port_rows)
        return getBoundary(lane.port_keys, row);
    row -= lane.port_rows;
    for (const auto & chunk : lane.chunks)
    {
        if (isVirtualRow(chunk))
            continue;
        if (row < chunk.getNumRows())
            return getBoundary(chunk, row);
        row -= chunk.getNumRows();
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Held row {} is beyond the rows held by the lane", row);
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

std::optional<Columns> VirtualRowReadAheadTransform::coverageBoundary()
{
    if (!limit)
        return std::nullopt;

    /// The merge pulls the next chunk of a lane only once it has consumed the previous one, so
    /// the last chunk it took is still ahead of it, as is a chunk waiting in the output port.
    auto held_rows = [](const Lane & lane)
    {
        return lane.taken_rows + lane.port_rows + lane.buffered_rows;
    };

    data_lanes.clear();
    for (size_t i = 0; i < lanes.size(); ++i)
        if (!lanes[i].finished && held_rows(lanes[i]) && !lanes[i].boundary.empty())
            data_lanes.push_back(i);

    /// Every held row of a lane is at or before its boundary, so in boundary order the rows of
    /// the lanes before the one reaching the limit all precede that lane's boundary. Inside the
    /// reaching lane only the rows up to the limit are needed, so the key of the last of them,
    /// or the previous lane's boundary if that is later, already covers `limit` held rows.
    std::sort(data_lanes.begin(), data_lanes.end(), [this](size_t lhs, size_t rhs) { return earlier(lhs, rhs); });
    size_t covered_rows = 0;
    const Columns * previous = nullptr;
    for (size_t lane_num : data_lanes)
    {
        const auto & lane = lanes[lane_num];
        size_t rows = held_rows(lane);
        if (covered_rows + rows >= limit)
        {
            Columns coverage = heldRowKey(lane, limit - covered_rows - 1);
            if (previous && compareBoundaries(*previous, coverage) > 0)
                return *previous;
            return coverage;
        }
        covered_rows += rows;
        previous = &lane.boundary;
    }
    return std::nullopt;
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

        /// The merge took the chunk from the port: it replaces the one the merge was consuming.
        if (lane.port_filled && !output.hasData())
        {
            lane.taken_rows = lane.port_rows;
            lane.taken_keys = std::move(lane.port_keys);
            lane.port_rows = 0;
            lane.port_filled = false;
        }

        /// An announcement always precedes the data read after it. As in
        /// `BufferChunksTransform`, the queue budget excludes the output port.
        if (output.canPush() && !lane.chunks.empty())
        {
            auto & next = lane.chunks.front();
            lane.port_keys.clear();
            if (!isVirtualRow(next))
            {
                lane.buffered_rows -= next.getNumRows();
                lane.buffered_bytes -= next.bytes();
                lane.demanded = true;
                lane.port_rows = next.getNumRows();
                for (size_t position : sort_positions)
                    lane.port_keys.push_back(next.getColumns()[position]);
            }
            lane.port_filled = true;
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
            /// An unconsumed announcement already answers the merge's next request for this
            /// lane; reading on before it is taken only replaces it and keeps the reader busy.
            bool announcement_pending = !lane.chunks.empty() && isVirtualRow(lane.chunks.back());
            if (lane.demanded)
            {
                if (!announcement_pending)
                    lane.read_requested = true;
            }
            else if (!lane.boundary.empty())
                candidates.push_back(i);
        }
    }

    /// Account for completed reads before granting more speculative work.
    if (ready_lanes.empty() && read_ahead_window && read_ahead_started)
    {
        /// Held rows alone may already satisfy the limit up to some boundary; the merge
        /// consumes them before it needs anything announced past that point, so reading
        /// there is wasted if the limit ends the query. Lanes below the point stay eligible.
        std::optional<Columns> coverage = coverageBoundary();
        if (coverage)
            std::erase_if(candidates, [&](size_t lane_num)
            {
                return compareBoundaries(lanes[lane_num].boundary, *coverage) > 0;
            });

        /// Lanes already holding a slot may keep filling their buffers. Only the free
        /// slots go to lanes that have not read anything yet, earliest boundary first.
        auto fresh = std::partition(candidates.begin(), candidates.end(),
            [this](size_t lane_num)
            {
                return lanes[lane_num].buffered_rows > 0;
            });
        for (auto it = candidates.begin(); it != fresh; ++it)
            lanes[*it].read_requested = lanes[*it].speculated = true;

        size_t free_slots = read_ahead_window > occupied_slots ? read_ahead_window - occupied_slots : 0;
        size_t count = std::min<size_t>(free_slots, candidates.end() - fresh);
        /// Without a coverage boundary the limit gives no way to tell a needed lane from a
        /// wasted one, so fan out only as far as sparse results have justified.
        if (limit && !coverage)
            count = std::min(count, speculation_allowance);
        std::partial_sort(fresh, fresh + count, candidates.end(),
            [this](size_t lhs, size_t rhs)
            {
                return earlier(lhs, rhs);
            });
        for (auto it = fresh; it != fresh + count; ++it)
            lanes[*it].read_requested = lanes[*it].speculated = true;
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
                /// A filter upstream drops the chunks it empties, so a speculative read that kept
                /// too few rows may show up as this announcement alone.
                if (lane.speculated && lane.rows_since_boundary < useful_rows_target)
                    speculation_allowance = std::min(read_ahead_window, speculation_allowance * 2);
                lane.speculated = false;
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
            /// A sparse speculative result is evidence that the lanes beyond it are needed too.
            /// The merge may have demanded the lane meanwhile; the read was still a speculation.
            if (lane.speculated && rows < useful_rows_target)
                speculation_allowance = std::min(read_ahead_window, speculation_allowance * 2);
            lane.speculated = false;
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
