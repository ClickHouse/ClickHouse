#include <Storages/MergeTree/MergeTreeInOrderSliceRouter.h>

#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>
#include <Storages/MergeTree/MergeTreeSliceEndInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeInOrderSliceRouter::MergeTreeInOrderSliceRouter(
    SharedHeader header,
    std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_,
    ExpressionActionsPtr virtual_row_conversions_)
    : IProcessor(InputPorts(pool_->numSources(), header), OutputPorts(pool_->numLanes(), header))
    , pool(std::move(pool_))
    , virtual_row_conversions(std::move(virtual_row_conversions_))
    , lanes(pool->numLanes())
    , assignments(pool->numSources())
{
    for (auto & input : inputs)
        source_inputs.push_back(&input);
    for (auto & output : outputs)
        lane_outputs.push_back(&output);
}

void MergeTreeInOrderSliceRouter::initialize()
{
    initialized = true;
    if (!virtual_row_conversions)
        return;

    const auto & header = outputs.front().getHeader();
    for (size_t lane = 0; lane < lanes.size(); ++lane)
    {
        const Block & boundary = pool->laneBoundary(lane);
        if (boundary.columns() == 0)
            continue;

        Columns empty_columns;
        empty_columns.reserve(header.columns());
        for (const auto & column : header)
            empty_columns.push_back(column.type->createColumn());

        Chunk chunk(std::move(empty_columns), 0);
        chunk.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(/*part_level=*/ 0, boundary, virtual_row_conversions));
        lanes[lane].initial_virtual_row = std::move(chunk);
    }
}

IProcessor::Status MergeTreeInOrderSliceRouter::prepare()
{
    if (!initialized)
        initialize();

    for (size_t source = 0; source < source_inputs.size(); ++source)
        if (source_inputs[source]->hasData())
            consumeInput(source);

    for (size_t lane = 0; lane < lanes.size(); ++lane)
        pushToLane(lane);

    if (num_finished_lanes == lanes.size())
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

    scheduleSlices();

    for (const auto & assignment : assignments)
        if (assignment)
            return Status::NeedData;
    return Status::PortFull;
}

IProcessor::Status MergeTreeInOrderSliceRouter::finish()
{
    /// A source still reading a slice is cut short: no lane wants its rows anymore.
    for (const auto & assignment : assignments)
    {
        if (assignment)
        {
            for (auto & input : inputs)
                input.close();
            return Status::Finished;
        }
    }

    /// Idle sources end their streams themselves once the pool has nothing more for them, so that
    /// they finish the way every source does (onFinish: statistics and logs).
    pool->finish();
    bool all_sources_finished = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;
        all_sources_finished = false;
        input.setNeeded();
    }
    return all_sources_finished ? Status::Finished : Status::NeedData;
}

void MergeTreeInOrderSliceRouter::consumeInput(size_t source)
{
    auto & input = *source_inputs[source];
    Chunk chunk = input.pull();
    const bool slice_ended = chunk.getChunkInfos().get<MergeTreeSliceEndInfo>() != nullptr;

    auto & assignment = assignments[source];
    if (!assignment)
    {
        /// A source without a slice can only report that it has nothing to read.
        if (!slice_ended)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got data from source {} that has no slice assigned", source);
        input.setNotNeeded();
        return;
    }

    /// The source asked the pool before this slice was assigned and found nothing: the report is stale,
    /// the source stays needed to pick the slice up.
    if (slice_ended && pool->hasPendingSlice(source))
        return;

    auto & lane = lanes[assignment->lane];

    /// The merge finished the lane while the slice was being read: nothing waits for its rows.
    if (lane.finished)
    {
        if (slice_ended)
        {
            input.setNotNeeded();
            assignment.reset();
        }
        return;
    }

    auto slice = lane.slices.find(assignment->first_mark);
    if (slice == lane.slices.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice starting at mark {} of lane {} is not issued", assignment->first_mark, assignment->lane);

    if (!slice_ended)
    {
        assignment->rows_read += chunk.getNumRows();
        slice->second.chunks.push_back(std::move(chunk));
        return;
    }

    slice->second.finished = true;
    if (slice->second.chunks.empty())
        dropSlice(assignment->lane, slice);
    input.setNotNeeded();

    /// Most rows of the slice were filtered out: reading is the bottleneck, not merging.
    if (assignment->rows_read * 4 < assignment->rows_in_marks)
        ++misses;

    assignment.reset();
}

void MergeTreeInOrderSliceRouter::dropSlice(size_t lane, SliceBuffers::iterator slice)
{
    issued_marks -= slice->second.marks;
    lanes[lane].slices.erase(slice);
}

void MergeTreeInOrderSliceRouter::finishLane(size_t lane_idx)
{
    auto & lane = lanes[lane_idx];
    lane.finished = true;
    ++num_finished_lanes;
    for (const auto & [first_mark, slice] : lane.slices)
        issued_marks -= slice.marks;
    lane.slices.clear();
    pool->finishLane(lane_idx);
}

void MergeTreeInOrderSliceRouter::pushToLane(size_t lane_idx)
{
    auto & lane = lanes[lane_idx];
    auto & output = *lane_outputs[lane_idx];
    lane.wants_data = false;
    if (lane.finished)
        return;

    if (output.isFinished())
    {
        finishLane(lane_idx);
        return;
    }

    if (!output.canPush())
        return;

    if (lane.initial_virtual_row)
    {
        output.push(std::move(*lane.initial_virtual_row));
        lane.initial_virtual_row.reset();
        return;
    }

    /// Rows leave a lane through its first issued slice only, so the lane stays in mark order.
    auto head = lane.slices.begin();
    if (head != lane.slices.end() && !head->second.chunks.empty())
    {
        Chunk chunk = std::move(head->second.chunks.front());
        head->second.chunks.pop_front();
        if (head->second.finished && head->second.chunks.empty())
            dropSlice(lane_idx, head);
        output.push(std::move(chunk));
        return;
    }

    if (lane.slices.empty() && !pool->laneHasUnreadMarks(lane_idx))
    {
        output.finish();
        finishLane(lane_idx);
        return;
    }

    lane.wants_data = true;
}

size_t MergeTreeInOrderSliceRouter::readAheadMarks() const
{
    /// Marks, not slices: the slices of a lane grow with every miss as well, so a query answered by the
    /// first granules of a lane never reads past the slice the merge waits for, whatever the thread count.
    if (misses == 0)
        return 0;
    const size_t all_sources_busy = assignments.size() * pool->maxSliceMarks();
    return std::min(all_sources_busy, size_t(1) << std::min<size_t>(misses, 40));
}

std::optional<size_t> MergeTreeInOrderSliceRouter::pickIdleSource(size_t lane) const
{
    /// A source that read the lane last still holds its readers, so it continues the lane for free.
    std::optional<size_t> idle;
    for (size_t source = 0; source < assignments.size(); ++source)
    {
        if (assignments[source])
            continue;
        if (pool->lastTaskLane(source) == lane)
            return source;
        if (!idle)
            idle = source;
    }
    return idle;
}

void MergeTreeInOrderSliceRouter::assignSlice(size_t source, size_t lane_idx)
{
    auto description = pool->assignSlice(source, lane_idx);

    auto [it, inserted] = lanes[lane_idx].slices.try_emplace(description.first_mark);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice starting at mark {} of lane {} was assigned twice", description.first_mark, lane_idx);

    it->second.marks = description.marks;
    issued_marks += description.marks;
    assignments[source] = Assignment{.lane = lane_idx, .first_mark = description.first_mark, .rows_in_marks = description.rows};
    source_inputs[source]->setNeeded();
}

void MergeTreeInOrderSliceRouter::scheduleSlices()
{
    /// The lane the merge is blocked on is read whatever the read-ahead depth: those rows are never waste.
    bool merge_waits = false;
    for (size_t lane = 0; lane < lanes.size(); ++lane)
    {
        if (!lanes[lane].wants_data)
            continue;

        merge_waits = true;
        if (!lanes[lane].slices.empty() || !pool->laneHasUnreadMarks(lane))
            continue;

        auto source = pickIdleSource(lane);
        if (!source)
            return;
        assignSlice(*source, lane);

        /// Lanes whose next key lies within the slice just issued are consumed before that slice is done:
        /// reading them now costs no more rows than waiting for the merge to ask for each of them in turn.
        while (auto before = pool->nextLaneBefore(lane))
        {
            source = pickIdleSource(*before);
            if (!source)
                return;
            assignSlice(*source, *before);
        }
    }

    if (!merge_waits)
        return;

    /// Read ahead in the order the merge is going to need the data.
    const size_t budget = readAheadMarks();
    while (issued_marks < budget)
    {
        auto lane = pool->nextLane();
        if (!lane)
            return;

        auto source = pickIdleSource(*lane);
        if (!source)
            return;
        assignSlice(*source, *lane);
    }
}

}
