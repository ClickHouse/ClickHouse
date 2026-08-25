#include <Processors/Transforms/VirtualRowReadAheadTransform.h>

#include <Columns/IColumn.h>
#include <Interpreters/ExpressionActions.h>
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
    , description(std::move(description_))
    , apply_virtual_row_conversions(apply_virtual_row_conversions_)
    , limit(limit_)
    , max_rows_to_buffer(max_rows_to_buffer_)
    , max_bytes_to_buffer(max_bytes_to_buffer_)
    , read_ahead_window(read_ahead_window_)
{
    for (const auto & desc : description)
        if (desc.collator)
            has_collation = true;

    lanes.resize(num_lanes);
    size_t i = 0;
    for (auto & input : inputs)
        lanes[i++].input = &input;
    i = 0;
    for (auto & output : outputs)
        lanes[i++].output = &output;
}

Columns VirtualRowReadAheadTransform::extractBoundary(const Chunk & chunk) const
{
    /// The chunk itself is left untouched (the merge downstream materializes it): the sort
    /// key is computed from a copy of the small pk block carried by the chunk info, without
    /// cloning the chunk or rebuilding the full header layout.
    const auto read_info = chunk.getChunkInfos().get<MergeTreeReadInfo>();
    chassert(read_info);

    Block pk_block = read_info->pk_block;
    if (apply_virtual_row_conversions)
        read_info->virtual_row_conversions->execute(pk_block);

    Columns boundary;
    boundary.reserve(description.size());
    for (const auto & desc : description)
    {
        const auto * pk_col = pk_block.findByName(desc.column_name);
        if (!pk_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Virtual row does not cover sort column {} (pk block: {})", desc.column_name, pk_block.dumpStructure());
        boundary.push_back(pk_col->column);
    }
    return boundary;
}

bool VirtualRowReadAheadTransform::boundaryLess(const Lane & lhs, const Lane & rhs) const
{
    for (size_t i = 0; i < description.size(); ++i)
    {
        int res = description[i].direction * lhs.boundary[i]->compareAt(0, 0, *rhs.boundary[i], description[i].nulls_direction);
        if (res != 0)
            return res < 0;
    }
    return false;
}

void VirtualRowReadAheadTransform::grantCredit(Lane & lane)
{
    lane.credit = 1;
    lane.input->setNeeded();
}

void VirtualRowReadAheadTransform::topUpReadAhead()
{
    size_t reading_ahead = 0;
    for (const auto & lane : lanes)
        if (lane.credit > 0 && !lane.input->isFinished())
            ++reading_ahead;

    if (reading_ahead >= read_ahead_window)
        return;

    std::vector<size_t> candidates;
    for (size_t i = 0; i < lanes.size(); ++i)
    {
        const auto & lane = lanes[i];
        /// A parked lane typically still holds its unconsumed virtual row and possibly some
        /// filtered-down real data; it may keep reading ahead while the real data stays
        /// under the buffer caps (the depth bound buffering always had).
        if (lane.credit == 0 && !lane.boundary.empty() && underBufferCaps(lane)
            && !lane.input->isFinished() && !lane.output->isFinished())
            candidates.push_back(i);
    }

    size_t slots = std::min(read_ahead_window - reading_ahead, candidates.size());
    if (slots == 0)
        return;

    /// Wake the parked lanes the merge will reach first.
    std::partial_sort(candidates.begin(), candidates.begin() + slots, candidates.end(),
        [&](size_t a, size_t b) { return boundaryLess(lanes[a], lanes[b]); });

    for (size_t i = 0; i < slots; ++i)
        grantCredit(lanes[candidates[i]]);
}

void VirtualRowReadAheadTransform::onMiss(size_t lane_num)
{
    auto & lane = lanes[lane_num];

    /// Demand-driven read: always allowed, this is not speculation.
    if (lane.credit == 0)
        grantCredit(lane);
    else
        lane.input->setNeeded();

    /// Before the initial virtual row the demand comes from the merge initialization,
    /// which asks every lane at once and says nothing about where the merge will go.
    if (lane.boundary.empty())
        return;

    if (limit == 0)
        cross_lane_read_ahead = true;

    if (first_miss_lane < 0)
        first_miss_lane = static_cast<ssize_t>(lane_num);
    else if (first_miss_lane != static_cast<ssize_t>(lane_num))
        cross_lane_read_ahead = true;

    if (cross_lane_read_ahead && speculationAllowed())
        topUpReadAhead();
}

bool VirtualRowReadAheadTransform::processLane(size_t lane_num)
{
    auto & lane = lanes[lane_num];
    auto & input = *lane.input;
    auto & output = *lane.output;

    if (output.isFinished())
    {
        input.close();
        lane.buffer = {};
        return false;
    }

    bool progress = false;

    /// Serve demand from the buffer.
    if (output.canPush())
    {
        if (!lane.buffer.empty())
        {
            Chunk chunk = std::move(lane.buffer.front());
            lane.buffer.pop();
            bool is_virtual_row = isVirtualRow(chunk);

            if (!is_virtual_row)
            {
                lane.buffered_rows -= chunk.getNumRows();
                lane.buffered_bytes -= chunk.bytes();
            }

            output.push(std::move(chunk));
            progress = true;

            /// Refill: keep a lane that demonstrably feeds the merge reading ahead, so its
            /// next read overlaps with the merge of the data just delivered. A virtual row
            /// is an announcement, not fed data — refilling on it would read a block from
            /// every source the merge only glanced at.
            if (!is_virtual_row && speculationAllowed() && lane.credit == 0 && underBufferCaps(lane)
                && !lane.boundary.empty() && !input.isFinished())
                grantCredit(lane);
        }
        else if (input.isFinished())
        {
            output.finish();
            return progress;
        }
        else
        {
            onMiss(lane_num);
        }
    }

    /// Pull from the input while the lane is allowed to read.
    if (lane.credit > 0 && underBufferCaps(lane))
    {
        input.setNeeded();
        if (input.hasData())
        {
            /// Pull with set_not_needed: when this chunk turns out to consume the last
            /// credit (a virtual row), a dangling "needed" would let the source read one
            /// speculative block before the port could be parked. Demand is re-announced
            /// below only while the credit lasts; skipping the version update also avoids
            /// waking the upstream just to learn the port is parked.
            Chunk chunk = input.pull(/*set_not_needed=*/ true);
            progress = true;

            if (isVirtualRow(chunk))
            {
                lane.boundary = extractBoundary(chunk);
                --lane.credit;
                lane.buffer.push(std::move(chunk));

                /// A read-ahead slot was freed: keep the window full so the lanes closest
                /// to the merge keep reading concurrently.
                if (cross_lane_read_ahead && speculationAllowed())
                    topUpReadAhead();
            }
            else if (chunk.getNumRows() == 0)
            {
                /// The merge drops empty chunks anyway; dropping them here keeps them
                /// inside their read-ahead group.
            }
            else
            {
                lane.num_processed_rows += chunk.getNumRows();
                lane.buffered_rows += chunk.getNumRows();
                lane.buffered_bytes += chunk.bytes();
                compactReplicatedColumns(chunk);
                lane.buffer.push(std::move(chunk));

                if (limit && lane.num_processed_rows >= limit)
                    input.close();
            }

            /// While the credit lasts, let the source work on its next chunk into the port
            /// (the port is the one-chunk pipelining slot; pulling it into the buffer stays
            /// gated by the caps). Only the end of a group must park the lane instantly.
            if (lane.credit > 0 && !input.isFinished())
                input.setNeeded();
        }
    }
    else if (lane.credit == 0)
    {
        input.setNotNeeded();
    }

    return progress;
}

IProcessor::Status VirtualRowReadAheadTransform::prepare()
{
    bool all_outputs_finished = true;
    for (const auto & lane : lanes)
    {
        if (!lane.output->isFinished())
        {
            all_outputs_finished = false;
            break;
        }
    }

    if (all_outputs_finished)
    {
        for (auto & lane : lanes)
            lane.input->close();
        return Status::Finished;
    }

    /// A pull can unblock a push on the same lane and vice versa, and our own port updates do
    /// not reschedule this processor, so iterate to a fixpoint before going back to sleep.
    bool progress = true;
    while (progress)
    {
        progress = false;
        for (size_t i = 0; i < lanes.size(); ++i)
            progress |= processLane(i);
    }

    bool all_lanes_done = true;
    for (const auto & lane : lanes)
    {
        if (!lane.output->isFinished() && !(lane.input->isFinished() && lane.buffer.empty()))
        {
            all_lanes_done = false;
            break;
        }
    }

    if (all_lanes_done)
    {
        for (auto & lane : lanes)
        {
            if (!lane.output->isFinished())
                lane.output->finish();
            lane.input->close();
        }
        return Status::Finished;
    }

    return Status::NeedData;
}

}
