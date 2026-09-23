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

    /// The source asked the pool before this slice was assigned and found nothing: the report is stale,
    /// the source stays needed to pick the slice up.
    if (slice_ended && pool->hasPendingSlice(source))
        return;

    auto & lane = lanes[assignment->lane];
    auto & slice = lane.slices.at(assignment->first_mark);

    if (!slice_ended)
    {
        assignment->rows_read += chunk.getNumRows();
        lane.buffered_rows += chunk.getNumRows();
        slice.chunks.push_back(std::move(chunk));
        return;
    }

    slice.finished = true;
    input.setNotNeeded();

    /// Most rows of the slice were filtered out: the lane is bound by reading, not by merging.
    /// One miss on the first, tiny slice of a lane is weak evidence, so looking into other lanes
    /// starts with the second miss.
    if (assignment->rows_read * 4 < assignment->rows_in_marks)
    {
        ++misses;
        speculation_open = misses >= 2;
        if (lane.activated)
            lane.max_sources = std::min(lane.max_sources * 2, assignments.size());
    }

    assignment.reset();
}

MergeTreeInOrderSliceRouter::SliceBuffer * MergeTreeInOrderSliceRouter::headSliceWithData(size_t lane)
{
    auto & slices = lanes[lane].slices;
    while (!slices.empty() && slices.begin()->second.finished && slices.begin()->second.chunks.empty())
        slices.erase(slices.begin());

    if (slices.empty() || slices.begin()->second.chunks.empty())
        return nullptr;

    return &slices.begin()->second;
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

bool MergeTreeInOrderSliceRouter::laneWantsMore(size_t lane) const
{
    /// Read ahead only while the consumer accepts rows, like a plain source that cannot read its next
    /// block before the previous one was pulled. With buffering downstream this still reads ahead one
    /// buffer per lane; without it, a lane the merge is not asking for is left alone.
    return lanes[lane].activated && !lanes[lane].finished && lane_outputs[lane]->canPush()
        && lanes[lane].buffered_rows < buffer_budget_rows;
}

size_t MergeTreeInOrderSliceRouter::sourcesOf(size_t lane) const
{
    size_t count = 0;
    for (size_t source = 0; source < assignments.size(); ++source)
        if (pool->sourceLane(source) == lane)
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
        size_t lane = *pool->sourceLane(source);
        return std::make_pair(!lanes[lane].activated, boundary_position[lane]);
    };

    for (size_t source = 0; source < assignments.size(); ++source)
    {
        if (assignments[source])
            continue;

        auto lane = pool->sourceLane(source);
        if (!lane || !pool->laneHasUnreadMarks(*lane))
            return source;

        /// Idle but bound to a lane that does not want to be read right now. Prefer taking it from a lane
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
    /// Sources continue the lane they are bound to.
    for (size_t source = 0; source < assignments.size(); ++source)
    {
        if (assignments[source])
            continue;

        auto lane = pool->sourceLane(source);
        if (lane && pool->laneHasUnreadMarks(*lane) && laneWantsMore(*lane))
            assignSlice(source, *lane);
    }

    /// Lanes the merge asked for get more sources, up to their cap, in the order the merge needs them.
    for (size_t lane : pool->lanesByBoundary())
    {
        while (laneWantsMore(lane) && pool->laneHasUnreadMarks(lane) && sourcesOf(lane) < lanes[lane].max_sources)
        {
            auto source = pickIdleSource(/*allow_rebinding=*/ true);
            if (!source)
                return;

            pool->bindSource(*source, lane);
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

        pool->bindSource(*source, lane);
        assignSlice(*source, lane);
    }
}

}
