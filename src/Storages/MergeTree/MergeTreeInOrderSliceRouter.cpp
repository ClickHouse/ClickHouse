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
    ExpressionActionsPtr virtual_row_conversions_,
    size_t limit_,
    size_t max_block_size_rows_)
    : IProcessor(InputPorts(pool_->numSources(), header), OutputPorts(pool_->numLanes(), header))
    , pool(std::move(pool_))
    , virtual_row_conversions(std::move(virtual_row_conversions_))
    , limit(limit_)
    , buffer_budget_rows(2 * max_block_size_rows_)
    , lanes(pool->numLanes())
    , assignments(pool->numSources())
    , boundary_position(pool->numLanes())
{
    for (auto & input : inputs)
        source_inputs.push_back(&input);
    for (auto & output : outputs)
        lane_outputs.push_back(&output);

    const auto & order = pool->lanesByBoundary();
    for (size_t position = 0; position < order.size(); ++position)
        boundary_position[order[position]] = position;
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
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    scheduleSlices();

    for (const auto & assignment : assignments)
        if (assignment)
            return Status::NeedData;
    return Status::PortFull;
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

    auto & lane = lanes[assignment->lane];
    auto & slice = lane.slices.at(assignment->first_mark);

    if (!slice_ended)
    {
        assignment->rows_read += chunk.getNumRows();
        lane.buffered_rows += chunk.getNumRows();
        slice.rows += chunk.getNumRows();
        slice.chunks.push_back(std::move(chunk));
        return;
    }

    slice.finished = true;
    input.setNotNeeded();

    /// Most rows of the slice were filtered out: the lane is bound by reading, not by merging.
    if (assignment->rows_read * 2 < assignment->rows_in_marks)
    {
        speculation_open = true;
        if (lane.activated)
            lane.max_segments = std::min(lane.max_segments * 2, assignments.size());
    }

    assignment.reset();
}

MergeTreeInOrderSliceRouter::SliceBuffer * MergeTreeInOrderSliceRouter::headSliceWithData(size_t lane)
{
    auto & slices = lanes[lane].slices;
    while (!slices.empty() && slices.begin()->second.finished && slices.begin()->second.chunks.empty())
        slices.erase(slices.begin());

    if (slices.empty())
        return nullptr;

    auto & [first_mark, slice] = *slices.begin();

    /// Marks before this slice are unread again (their segment was taken from an idle source): wait for them.
    if (first_mark >= pool->laneFirstUnreadMark(lane))
        return nullptr;

    return slice.chunks.empty() ? nullptr : &slice;
}

void MergeTreeInOrderSliceRouter::pushToLane(size_t lane_idx)
{
    auto & lane = lanes[lane_idx];
    auto & output = *lane_outputs[lane_idx];
    if (lane.finished)
        return;

    if (output.isFinished())
    {
        lane.finished = true;
        ++num_finished_lanes;
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

    if (SliceBuffer * slice = headSliceWithData(lane_idx))
    {
        Chunk chunk = std::move(slice->chunks.front());
        slice->chunks.pop_front();
        slice->rows -= chunk.getNumRows();
        lane.buffered_rows -= chunk.getNumRows();
        lane.delivered_rows += chunk.getNumRows();
        output.push(std::move(chunk));
        return;
    }

    if (lane.slices.empty() && !pool->laneHasUnreadMarks(lane_idx))
    {
        output.finish();
        lane.finished = true;
        ++num_finished_lanes;
        return;
    }

    /// The merge waits for this lane and nothing is ready for it.
    lane.activated = true;
}

size_t MergeTreeInOrderSliceRouter::deliverableRows(size_t lane) const
{
    const size_t first_unread_mark = pool->laneFirstUnreadMark(lane);
    size_t rows = 0;
    for (const auto & [first_mark, slice] : lanes[lane].slices)
    {
        if (first_mark >= first_unread_mark)
            break;
        rows += slice.rows;
    }
    return rows;
}

bool MergeTreeInOrderSliceRouter::headWantsMore(size_t lane) const
{
    return lanes[lane].activated && !lanes[lane].finished && deliverableRows(lane) < buffer_budget_rows;
}

bool MergeTreeInOrderSliceRouter::laneWantsMore(size_t lane) const
{
    return lanes[lane].activated && !lanes[lane].finished && lanes[lane].buffered_rows < buffer_budget_rows;
}

size_t MergeTreeInOrderSliceRouter::openSegmentsOf(size_t lane) const
{
    size_t count = 0;
    for (size_t source = 0; source < assignments.size(); ++source)
        if (pool->segmentLane(source) == lane && (assignments[source] || pool->segmentHasUnreadMarks(source)))
            ++count;
    return count;
}

size_t MergeTreeInOrderSliceRouter::speculativeSlicesInFlight() const
{
    size_t count = 0;
    for (const auto & assignment : assignments)
        if (assignment && !lanes[assignment->lane].activated)
            ++count;
    return count;
}

bool MergeTreeInOrderSliceRouter::coverageAllows(size_t lane) const
{
    if (limit == 0)
        return true;

    /// Rows held before the lane's boundary already satisfy the LIMIT: the lane is not going to be needed.
    size_t rows_before = 0;
    for (size_t other : pool->lanesByBoundary())
    {
        if (other == lane)
            break;
        rows_before += lanes[other].delivered_rows + lanes[other].buffered_rows;
    }
    return rows_before < limit;
}

std::optional<size_t> MergeTreeInOrderSliceRouter::pickIdleSource(bool allow_rebinding) const
{
    std::optional<size_t> victim;
    auto victim_rank = [this](size_t source)
    {
        size_t lane = *pool->segmentLane(source);
        return std::make_pair(!lanes[lane].activated, boundary_position[lane]);
    };

    for (size_t source = 0; source < assignments.size(); ++source)
    {
        if (assignments[source])
            continue;

        if (!pool->segmentHasUnreadMarks(source))
            return source;

        /// Idle but bound to a segment its lane does not want read now. Prefer taking it from a lane
        /// the merge never asked for, then from the lane needed last.
        if (allow_rebinding && (!victim || victim_rank(source) > victim_rank(*victim)))
            victim = source;
    }
    return victim;
}

void MergeTreeInOrderSliceRouter::assignSlice(size_t source, size_t lane_idx)
{
    auto description = pool->assignSlice(source);

    auto & lane = lanes[lane_idx];
    auto [it, inserted] = lane.slices.try_emplace(description.first_mark);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice starting at mark {} of lane {} was assigned twice", description.first_mark, lane_idx);

    lane.touched = true;
    assignments[source] = Assignment{.lane = lane_idx, .first_mark = description.first_mark, .rows_in_marks = description.rows};
    source_inputs[source]->setNeeded();
}

void MergeTreeInOrderSliceRouter::scheduleSlices()
{
    /// Sources continue the segment they are bound to. The segment at the head of its lane produces the
    /// rows the merge consumes next, so it goes on while the rows ready for delivery are below the budget.
    /// The other segments read ahead and stop once the lane as a whole holds enough rows.
    for (size_t source = 0; source < assignments.size(); ++source)
    {
        if (assignments[source] || !pool->segmentHasUnreadMarks(source))
            continue;

        size_t lane = *pool->segmentLane(source);
        bool is_head = pool->segmentFirstUnreadMark(source) == pool->laneFirstUnreadMark(lane);
        if (is_head ? headWantsMore(lane) : laneWantsMore(lane))
            assignSlice(source, lane);
    }

    /// Lanes the merge asked for get more segments, in the order the merge needs them.
    for (size_t lane : pool->lanesByBoundary())
    {
        if (!lanes[lane].activated || lanes[lane].finished || !pool->laneHasMarksOutsideSegments(lane))
            continue;

        /// The head of the lane belongs to no segment (its segment was taken away): it must get one
        /// regardless of the cap, otherwise the rows buffered behind it could never be delivered.
        bool head_outside_segments = pool->laneFirstMarkOutsideSegments(lane) == pool->laneFirstUnreadMark(lane);
        if (head_outside_segments && headWantsMore(lane))
        {
            auto source = pickIdleSource(/*allow_rebinding=*/ true);
            if (!source)
                return;

            pool->openSegment(*source, lane);
            assignSlice(*source, lane);
        }

        while (laneWantsMore(lane) && openSegmentsOf(lane) < lanes[lane].max_segments && pool->laneHasMarksOutsideSegments(lane))
        {
            auto source = pickIdleSource(/*allow_rebinding=*/ true);
            if (!source)
                return;

            pool->openSegment(*source, lane);
            assignSlice(*source, lane);
        }
    }

    if (!speculation_open)
        return;

    /// Untouched lanes get one slice each so that their first rows are ready when the merge reaches them.
    for (size_t lane : pool->lanesByBoundary())
    {
        if (lanes[lane].touched || lanes[lane].finished)
            continue;

        if (speculativeSlicesInFlight() >= assignments.size() || !coverageAllows(lane))
            return;

        auto source = pickIdleSource(/*allow_rebinding=*/ false);
        if (!source)
            return;

        pool->openSegment(*source, lane);
        assignSlice(*source, lane);
    }
}

}
