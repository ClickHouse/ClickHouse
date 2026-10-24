#include <Storages/MergeTree/MergeTreeInOrderSliceRouter.h>

#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Storages/MergeTree/MergeTreeSliceInfo.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeInOrderSliceRouter::MergeTreeInOrderSliceRouter(
    SharedHeader header, std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_, ExpressionActionsPtr virtual_row_conversions_)
    : IProcessor(InputPorts(pool_->numSources(), header), OutputPorts(pool_->numLanes(), header))
    , pool(std::move(pool_))
    , virtual_row_conversions(std::move(virtual_row_conversions_))
    , max_marks_in_flight(pool->numSources() * pool->maxSliceMarks())
    , input_needed(pool->numSources(), false)
    , lanes(pool->numLanes())
{
    if (!virtual_row_conversions)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The sliced pool is read without virtual rows");

    for (auto & input : inputs)
        source_inputs.push_back(&input);
    for (auto & output : outputs)
        lane_outputs.push_back(&output);
}

void MergeTreeInOrderSliceRouter::dropSlice(size_t lane, Slices::iterator slice)
{
    marks_in_flight -= slice->second.marks;
    if (slice->second.rows == 0)
        empty_marks += slice->second.marks;
    else
        lanes[lane].taken_marks += slice->second.marks;
    lanes[lane].slices.erase(slice);
}

void MergeTreeInOrderSliceRouter::finishLane(size_t lane)
{
    for (const auto & [seq, slice] : lanes[lane].slices)
        marks_in_flight -= slice.marks;
    lanes[lane].slices.clear();
    lanes[lane].blocked = false;
    pool->finishLane(lane);
}

IProcessor::Status MergeTreeInOrderSliceRouter::prepare()
{
    /// Chunks of a finished lane are dropped: nothing waits for their rows.
    auto find_slice = [&](const MergeTreeSliceTag & tag)
    {
        auto it = lanes[tag.lane].slices.find(tag.seq);
        if (it == lanes[tag.lane].slices.end() && !lane_outputs[tag.lane]->isFinished())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice {} of lane {} is not cut", tag.seq, tag.lane);
        return it;
    };

    for (size_t source = 0; source < source_inputs.size(); ++source)
    {
        auto & input = *source_inputs[source];
        if (!input.hasData())
            continue;

        Chunk chunk = input.pull();
        const auto info = chunk.getChunkInfos().extract<MergeTreeSliceInfo>();
        if (!info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got a chunk without slice info from a source of the sliced pool");

        if (info->ended)
        {
            auto it = find_slice(*info->ended);
            if (it != lanes[info->ended->lane].slices.end())
            {
                it->second.complete = true;
                if (it->second.chunks.empty())
                    dropSlice(info->ended->lane, it);
            }
        }

        if (info->slice)
        {
            auto it = find_slice(*info->slice);
            if (it != lanes[info->slice->lane].slices.end())
            {
                it->second.rows += chunk.getNumRows();
                /// A chunk without rows is progress, not data, unless it is a virtual row of the source.
                if (chunk.getNumRows() > 0 || isVirtualRow(chunk))
                    it->second.chunks.push_back(std::move(chunk));
            }
        }

        /// An idle source is parked, unless slices were cut since it looked: then it stays needed and
        /// takes one when it asks again.
        if (info->idle && pool->fifoSize() == 0)
        {
            input.setNotNeeded();
            input_needed[source] = false;
        }
    }

    bool asked = false;
    MergeTreeReadPoolInOrderSliced::Demand demand;
    for (size_t lane = 0; lane < lane_outputs.size(); ++lane)
    {
        auto & output = *lane_outputs[lane];
        if (output.isFinished())
        {
            finishLane(lane);
            continue;
        }

        if (!output.canPush())
            continue;

        /// The merge asks for the lane: whatever it took in full before this, it went through and wants
        /// more. A merge that stops after a chunk never comes back, and its slices never count.
        auto & lane_state = lanes[lane];
        asked |= !std::exchange(lane_state.blocked, false);
        consumed_marks += std::exchange(lane_state.taken_marks, 0);

        /// Rows leave a lane through its first slice only, so the lane stays in reading order.
        auto head = lane_state.slices.begin();
        if (head != lane_state.slices.end() && !head->second.chunks.empty())
        {
            output.push(std::move(head->second.chunks.front()));
            head->second.chunks.pop_front();
            if (head->second.complete && head->second.chunks.empty())
                dropSlice(lane, head);
            continue;
        }

        /// Nothing is ready: the rows of the slice in flight start at its boundary mark, else the lane's
        /// rows start at its next unread mark, and the lane is done when there is none.
        std::optional<size_t> next_mark;
        if (head != lane_state.slices.end())
            next_mark = head->second.boundary_mark;
        else
            next_mark = pool->nextUnreadMark(lane);

        if (!next_mark)
        {
            spans_lanes = true;
            output.finish();
            finishLane(lane);
            continue;
        }

        Block key = pool->keyAtMark(lane, *next_mark);
        if (key.columns() != 0 && MergeTreeReadPoolInOrderSliced::compareKeys(key, lane_state.announced_key) != 0)
        {
            Chunk announcement(output.getHeader().cloneEmptyColumns(), 0);
            announcement.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(/*part_level=*/ 0, key, virtual_row_conversions));
            lane_state.announced_key = std::move(key);
            output.push(std::move(announcement));
            continue;
        }

        lane_state.blocked = true;
        if (head == lane_state.slices.end())
            demand.blocked_lanes.push_back(lane);
    }

    if (std::ranges::all_of(outputs, [](const OutputPort & output) { return output.isFinished(); }))
        return finish();

    /// A source ends its stream on its own only when reading was cancelled for a partial result. The
    /// rows of its slice are not coming, so no lane can be completed in order anymore: end them all,
    /// the way a cancelled source ends its stream.
    for (const auto & input : inputs)
    {
        if (input.isFinished())
        {
            for (auto & output : outputs)
                output.finish();
            for (auto & other : inputs)
                other.close();
            return Status::Finished;
        }
    }

    std::vector<size_t> parked;
    for (size_t source = 0; source < source_inputs.size(); ++source)
        if (!input_needed[source])
            parked.push_back(source);

    /// Read-ahead only on the merge's demand, a lane asked for anew or waited for: a merge that stops
    /// after the chunk it just took (a LIMIT) must not trigger reads it never needs.
    const bool read_ahead = asked || std::ranges::any_of(lanes, [](const Lane & lane) { return lane.blocked; });
    const size_t budget = std::min(max_marks_in_flight, 4 * consumed_marks + 16 * empty_marks);
    if (read_ahead && budget > marks_in_flight)
        demand.read_ahead_marks = budget - marks_in_flight;
    demand.spans_lanes = spans_lanes;
    demand.lane_in_flight.resize(lanes.size());
    for (size_t lane = 0; lane < lanes.size(); ++lane)
        demand.lane_in_flight[lane] = !lanes[lane].slices.empty();

    auto scheduled = pool->schedule(demand, parked);
    for (const auto & cut : scheduled.cut)
    {
        lanes[cut.slice.lane].slices.emplace(cut.slice.seq, Slice{.marks = cut.marks, .boundary_mark = cut.boundary_mark});
        marks_in_flight += cut.marks;
    }
    for (size_t source : scheduled.sources_to_wake)
    {
        source_inputs[source]->setNeeded();
        input_needed[source] = true;
    }

    for (bool needed : input_needed)
        if (needed)
            return Status::NeedData;
    return Status::PortFull;
}

IProcessor::Status MergeTreeInOrderSliceRouter::finish()
{
    if (!finishing)
    {
        /// Sources still reading are cut short: no lane wants their rows anymore.
        for (bool needed : input_needed)
        {
            if (needed)
            {
                for (auto & input : inputs)
                    input.close();
                return Status::Finished;
            }
        }

        /// Idle sources end their streams themselves once the pool has nothing more for them, so that
        /// they finish the way every source does (onFinish: statistics and logs).
        finishing = true;
        pool->finish();
        for (auto & input : inputs)
            if (!input.isFinished())
                input.setNeeded();
    }

    for (const auto & input : inputs)
        if (!input.isFinished())
            return Status::NeedData;
    return Status::Finished;
}

}
