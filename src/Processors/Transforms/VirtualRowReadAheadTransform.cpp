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
    /// Boundary comparison in `boundaryLess` is collation-unaware; the sorting step does not
    /// insert this transform for collated sort descriptions.
    for (const auto & desc : description)
        chassert(!desc.collator);

    lanes.resize(num_lanes);
    lane_touch_epoch.resize(num_lanes, 0);
    size_t i = 0;
    for (auto & input : inputs)
    {
        lanes[i].input = &input;
        port_to_lane[&input] = i;
        ++i;
    }
    i = 0;
    for (auto & output : outputs)
    {
        lanes[i].output = &output;
        port_to_lane[&output] = i;
        ++i;
    }
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

bool VirtualRowReadAheadTransform::touchLane(size_t lane_num)
{
    if (lane_touch_epoch[lane_num] == touch_epoch)
        return false;
    lane_touch_epoch[lane_num] = touch_epoch;
    touched_lanes.push_back(lane_num);
    return true;
}

void VirtualRowReadAheadTransform::grantCredit(size_t lane_num, bool speculative)
{
    auto & lane = lanes[lane_num];
    lane.credit = 1;
    lane.speculative = speculative;
    lane.input->setNeeded();
    /// The input may already hold a chunk pushed just before the lane parked; the setNeeded
    /// above wakes only the upstream, so this prepare must process the lane too.
    credited_lanes.push_back(lane_num);
}

void VirtualRowReadAheadTransform::topUpReadAhead()
{
    size_t reading_ahead = 0;
    for (const auto & lane : lanes)
        if (lane.speculative && lane.credit > 0 && !lane.input->isFinished())
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
        grantCredit(candidates[i], /*speculative=*/ true);
}

void VirtualRowReadAheadTransform::onMiss(size_t lane_num)
{
    auto & lane = lanes[lane_num];

    /// Demand-driven read: always allowed, this is not speculation.
    if (lane.credit == 0)
        grantCredit(lane_num);
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
            if (!is_virtual_row)
                lane.speculative = false;

            /// Refill: keep the lane feeding the merge one read ahead — but not on a virtual
            /// row, and not past one still waiting in the buffer: reading beyond an announced
            /// boundary is the gated window's decision, not the refill's.
            if (!is_virtual_row && speculationAllowed() && lane.credit == 0 && underBufferCaps(lane)
                && !lane.boundary.empty() && !input.isFinished()
                && (lane.buffer.empty() || !isVirtualRow(lane.buffer.front())))
                grantCredit(lane_num);
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

                /// An unconsumed boundary announcement is obsoleted by a newer one (a virtual
                /// row promises "my next output starts at or after this key", and a later one
                /// only tightens that), so keep at most one trailing virtual row: this bounds
                /// the buffer while the lane scans through filtered-out groups and spares the
                /// merge a cursor round trip per obsolete boundary.
                if (!lane.buffer.empty() && isVirtualRow(lane.buffer.back()))
                    lane.buffer.back() = std::move(chunk);
                else
                    lane.buffer.push(std::move(chunk));

                --lane.credit;

                /// A read-ahead slot was freed: keep the window full so the lanes closest to
                /// the merge keep reading concurrently. A lane whose groups are entirely
                /// filtered out is typically re-granted right here and scans on; once its
                /// boundary runs ahead of the k nearest ones, it stops being selected — that
                /// is what bounds the reads wasted when a limit ends the merge early.
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
                /// Compact before measuring: the buffered bytes are decremented from the
                /// compacted chunk on the way out, so both sides must measure the same columns.
                compactReplicatedColumns(chunk);
                lane.num_processed_rows += chunk.getNumRows();
                lane.buffered_rows += chunk.getNumRows();
                lane.buffered_bytes += chunk.bytes();
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

IProcessor::Status VirtualRowReadAheadTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
        return prepare();

    ++touch_epoch;
    touched_lanes.clear();
    bool port_finished = false;

    for (const auto * port : updated_inputs)
    {
        port_finished |= port->isFinished();
        touchLane(port_to_lane.at(port));
    }
    for (const auto * port : updated_outputs)
    {
        port_finished |= port->isFinished();
        touchLane(port_to_lane.at(port));
    }

    /// A finished port changes the termination state, which only the full pass tracks; an
    /// unattributable wake-up (nothing touched) is answered with a full look for the same
    /// reason. Both are rare.
    if (port_finished || touched_lanes.empty())
        return prepare();

    /// A pull can unblock a push on the same lane and vice versa, and our own port updates do
    /// not reschedule this processor, so iterate to a fixpoint. Lanes outside the touched set
    /// are at their fixpoint already (every state transition here is driven by a port event)
    /// until a pass grants them credit; those join the set for the next pass.
    bool progress = true;
    while (progress)
    {
        progress = false;
        for (size_t lane_num : touched_lanes)
            progress |= processLane(lane_num);

        for (size_t lane_num : credited_lanes)
            progress |= touchLane(lane_num);
        credited_lanes.clear();
    }

    return tryFinish();
}

IProcessor::Status VirtualRowReadAheadTransform::tryFinish()
{
    /// Both prepare variants must end here: the terminal transition can be our own doing
    /// (finishing the last live output when its input drained), and a flag our side already
    /// set makes the downstream close() skip the version bump — no event would ever come
    /// back to conclude the processor.
    bool all_lanes_done = true;
    for (const auto & lane : lanes)
    {
        if (!lane.output->isFinished() && !(lane.input->isFinished() && lane.buffer.empty()))
        {
            all_lanes_done = false;
            break;
        }
    }

    if (!all_lanes_done)
        return Status::NeedData;

    for (auto & lane : lanes)
    {
        if (!lane.output->isFinished())
            lane.output->finish();
        lane.input->close();
    }
    return Status::Finished;
}

IProcessor::Status VirtualRowReadAheadTransform::prepare()
{
    initialized = true;

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
        /// Every lane is in the pass already.
        credited_lanes.clear();
    }

    return tryFinish();
}

}
