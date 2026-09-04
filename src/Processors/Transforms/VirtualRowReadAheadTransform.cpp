#include <Processors/Transforms/VirtualRowReadAheadTransform.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool VirtualRowReadAheadTransform::Lane::isDemanded() const
{
    return buffer.empty() && output->canPush();
}

bool VirtualRowReadAheadTransform::Lane::holdsVirtualRow() const
{
    return !buffer.empty() && isVirtualRow(buffer.back());
}

bool VirtualRowReadAheadTransform::Lane::underCaps(size_t max_rows, size_t max_bytes) const
{
    return (max_rows == 0 || buffered_rows < max_rows) && (max_bytes == 0 || buffered_bytes < max_bytes);
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
    , read_ahead_window(read_ahead_window_)
    , lanes(num_lanes)
    , ranked_lanes(BoundLess{this})
    , window_open(limit == 0)
{
    sort_column_positions.reserve(description.size());
    for (const auto & column : description)
        sort_column_positions.push_back(header->getPositionByName(column.column_name));

    size_t lane_num = 0;
    auto input = inputs.begin();
    auto output = outputs.begin();
    for (; input != inputs.end(); ++input, ++output, ++lane_num)
    {
        lanes[lane_num].input = &*input;
        lanes[lane_num].output = &*output;
        lane_by_port[&*input] = lane_num;
        lane_by_port[&*output] = lane_num;
    }
}

bool VirtualRowReadAheadTransform::BoundLess::operator()(size_t lhs, size_t rhs) const
{
    int cmp = self->compareKeys(self->lanes[lhs].bound, self->lanes[rhs].bound);
    if (cmp != 0)
        return cmp < 0;
    return lhs < rhs;
}

int VirtualRowReadAheadTransform::compareKeys(const Columns & lhs, const Columns & rhs) const
{
    for (size_t i = 0; i < description.size(); ++i)
    {
        int cmp = description[i].direction * lhs[i]->compareAt(0, 0, *rhs[i], description[i].nulls_direction);
        if (cmp != 0)
            return cmp;
    }
    return 0;
}

Columns VirtualRowReadAheadTransform::virtualRowKey(const Chunk & chunk) const
{
    auto info = chunk.getChunkInfos().get<MergeTreeReadInfo>();

    /// The merge converts the shared block in place; converting it here too would apply the expression twice.
    Block pk_block = info->pk_block;
    if (apply_virtual_row_conversions)
    {
        if (!info->virtual_row_conversions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Virtual row has no conversions to apply");
        info->virtual_row_conversions->execute(pk_block);
    }

    Columns key;
    key.reserve(description.size());
    for (size_t i = 0; i < description.size(); ++i)
    {
        const auto & pk_column = pk_block.getByName(description[i].column_name);
        const auto & header_column = header->getByPosition(sort_column_positions[i]);
        if (!header_column.type->equals(*pk_column.type))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Virtual row has different type for {}. Expected {}, got {}",
                header_column.name, header_column.type->getName(), pk_column.type->getName());
        key.push_back(pk_column.column->convertToFullIfWrapped());
    }
    return key;
}

Columns VirtualRowReadAheadTransform::lastRowKey(const Chunk & chunk) const
{
    const auto & columns = chunk.getColumns();
    size_t last_row = chunk.getNumRows() - 1;

    Columns key;
    key.reserve(sort_column_positions.size());
    for (size_t position : sort_column_positions)
        key.push_back(columns[position]->cut(last_row, 1)->convertToFullIfWrapped());
    return key;
}

/// A lane outside the set that can still read. A lane whose input is finished is neither in
/// the set nor a frontier: the merge drains its buffer without any further read, so data
/// beyond its bound is not premature for anyone.
bool VirtualRowReadAheadTransform::isFrontierCandidate(size_t lane_num) const
{
    const Lane & lane = lanes[lane_num];
    return !lane.in_set && !lane.input->isFinished();
}

/// The frontier as `lane_num` sees it. A lane reading outside the set (the one the merge is
/// draining, or any demanded lane when K = 0) may itself be the smallest-bound outsider, and
/// must not be stopped by its own bound: it takes the next one in order.
ssize_t VirtualRowReadAheadTransform::frontierFor(size_t lane_num) const
{
    if (frontier_lane != static_cast<ssize_t>(lane_num))
        return frontier_lane;

    auto it = ranked_lanes.find(lane_num);
    for (++it; it != ranked_lanes.end(); ++it)
        if (isFrontierCandidate(*it))
            return *it;
    return -1;
}

bool VirtualRowReadAheadTransform::passedFrontier(size_t lane_num) const
{
    ssize_t frontier = frontierFor(lane_num);
    return lanes[lane_num].ranked && frontier >= 0 && BoundLess{this}(static_cast<size_t>(frontier), lane_num);
}

/// Whether the lane may pull another chunk now. Demand is always served, a warm-up grant too;
/// otherwise only a set member with the window open, or the lane the merge is draining, reads,
/// and stops at the budget, at its caps, or once its bound passes the frontier.
bool VirtualRowReadAheadTransform::mayRead(size_t lane_num) const
{
    const Lane & lane = lanes[lane_num];
    if (lane.isDemanded() || lane.warmup_credit)
        return true;

    bool active = (window_open && lane.in_set) || last_demanded_lane == static_cast<ssize_t>(lane_num);
    if (!active)
        return false;

    if (limit && budget_rows >= limit)
        return false;

    if (!lane.underCaps(max_rows_to_buffer, max_bytes_to_buffer))
        return false;

    return !passedFrontier(lane_num);
}

void VirtualRowReadAheadTransform::enqueue(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.queued)
        return;
    lane.queued = true;
    candidates.push_back(lane_num);
}

/// Walks the lanes in bound order. The first K that can read now, i.e. under their caps, form
/// the set; the first one passed over because it is at its caps, or the (K + 1)-th, is the
/// frontier. Lanes whose input is finished are skipped altogether (see `isFrontierCandidate`).
/// The walk stops as soon as both are known, so it costs K + 1 steps and not N.
void VirtualRowReadAheadTransform::recomputeSet()
{
    std::swap(previous_set_lanes, set_lanes);
    for (size_t lane_num : previous_set_lanes)
        lanes[lane_num].in_set = false;

    set_lanes.clear();
    frontier_lane = -1;

    for (size_t lane_num : ranked_lanes)
    {
        Lane & lane = lanes[lane_num];
        if (lane.input->isFinished())
            continue;

        if (set_lanes.size() < read_ahead_window && lane.underCaps(max_rows_to_buffer, max_bytes_to_buffer))
        {
            lane.in_set = true;
            set_lanes.push_back(lane_num);
        }
        else if (frontier_lane < 0)
        {
            frontier_lane = lane_num;
        }

        if (frontier_lane >= 0 && set_lanes.size() == read_ahead_window)
            break;
    }

    /// Lanes that left must release their input; members must re-check the frontier and the budget.
    for (size_t lane_num : previous_set_lanes)
        enqueue(lane_num);
    for (size_t lane_num : set_lanes)
        enqueue(lane_num);
}

/// Drives every lane with something to do, then repeats while a pass has changed who may read,
/// so a single call settles the whole processor: the executor does not reschedule a processor
/// for its own port updates. Every raise of `set_stale` is a state transition of finite supply
/// (a chunk pulled or pushed, a lane finished, the window opened), so this ends.
void VirtualRowReadAheadTransform::runLanes()
{
    while (true)
    {
        if (set_stale)
        {
            set_stale = false;
            recomputeSet();
            if (last_demanded_lane >= 0)
                enqueue(last_demanded_lane);
        }

        /// `candidates` may grow while it is walked (warm-up grants).
        for (size_t pos = 0; pos < candidates.size(); ++pos)
            driveLane(candidates[pos]);

        for (size_t lane_num : candidates)
            lanes[lane_num].queued = false;
        candidates.clear();

        if (!set_stale)
            return;
    }
}

void VirtualRowReadAheadTransform::pushFromBuffer(Lane & lane)
{
    while (!lane.buffer.empty() && lane.output->canPush())
    {
        Chunk chunk = std::move(lane.buffer.front());
        lane.buffer.pop_front();

        if (!isVirtualRow(chunk))
        {
            lane.buffered_rows -= chunk.getNumRows();
            lane.buffered_bytes -= chunk.bytes();
            /// A lane parked at its caps may be under them again.
            if (!lane.in_set)
                invalidateSet();
        }

        lane.output->push(std::move(chunk));
    }
}

void VirtualRowReadAheadTransform::noteDemand(size_t lane_num)
{
    const Lane & lane = lanes[lane_num];
    /// The merge's initialisation demands every lane before any bound is known; those demands
    /// are served with the first chunk and grant nothing beyond it.
    if (!lane.ranked)
        return;

    /// The window opens when the merge moves on to a second lane parked behind a virtual row.
    if (limit && !window_open && lane.bound_from_virtual_row)
    {
        if (first_demanded_lane < 0)
        {
            first_demanded_lane = lane_num;
        }
        else if (first_demanded_lane != static_cast<ssize_t>(lane_num))
        {
            window_open = true;
            invalidateSet();
        }
    }

    if (last_demanded_lane != static_cast<ssize_t>(lane_num))
    {
        /// The previous holder loses its reading rights and must release its input.
        if (last_demanded_lane >= 0)
            enqueue(last_demanded_lane);
        last_demanded_lane = lane_num;
    }
}

/// Moves one lane as far as it can go right now: pushes what the merge will take, pulls while
/// the lane may read and data is there, and leaves the input port needed only if the lane may
/// still read (a port left needed lets the source read one block into it).
void VirtualRowReadAheadTransform::driveLane(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.output_finished)
        return;

    if (lane.output->isFinished())
    {
        finishLane(lane_num);
        return;
    }

    pushFromBuffer(lane);
    if (lane.isDemanded())
        noteDemand(lane_num);

    while (!lane.input->isFinished() && mayRead(lane_num))
    {
        lane.input->setNeeded();
        if (!lane.input->hasData())
            return;

        consume(lane_num, lane.input->pull(/* set_not_needed */ true));
        pushFromBuffer(lane);
    }

    if (lane.input->isFinished())
        onInputExhausted(lane_num);
    else
        lane.input->setNotNeeded();
}

void VirtualRowReadAheadTransform::onInputExhausted(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.buffer.empty())
    {
        finishLane(lane_num);
        return;
    }

    /// Nothing more to read: the lane leaves the set while the merge drains its buffer.
    if (!lane.exhausted_noted)
    {
        lane.exhausted_noted = true;
        invalidateSet();
    }
}

void VirtualRowReadAheadTransform::consume(size_t lane_num, Chunk chunk)
{
    Lane & lane = lanes[lane_num];

    if (isVirtualRow(chunk))
    {
        bool filtered_stretch = lane.announced && lane.rows_since_virtual_row == 0;
        lane.announced = true;
        lane.rows_since_virtual_row = 0;
        setBound(lane_num, virtualRowKey(chunk), /* from_virtual_row */ true);

        /// A newer announcement replaces a pending one: the merge needs only the latest.
        if (lane.holdsVirtualRow())
            lane.buffer.back() = std::move(chunk);
        else
            lane.buffer.push_back(std::move(chunk));

        if (filtered_stretch && limit && !window_open && last_demanded_lane == static_cast<ssize_t>(lane_num))
            grantWarmup(lane_num);
        return;
    }

    /// The warm-up grant is one block of data. A source never pushes a fully filtered block,
    /// only the virtual row after it, so the grant lasts through a filtered stretch: it is meant
    /// to leave the lane's first block buffered for the merge, and the filtered blocks pull no
    /// rows, which is the currency the waste bound is stated in.
    lane.warmup_credit = false;

    if (!chunk.hasRows())
        return;

    size_t rows = chunk.getNumRows();
    setBound(lane_num, lastRowKey(chunk), /* from_virtual_row */ false);
    lane.rows_pulled += rows;
    lane.rows_since_virtual_row += rows;

    bool budget_was_left = !limit || budget_rows < limit;
    budget_rows += rows;
    if (budget_was_left && limit && budget_rows >= limit)
        invalidateSet();

    /// Data supersedes the announcement ahead of it.
    if (lane.holdsVirtualRow())
        lane.buffer.pop_back();

    lane.buffered_rows += rows;
    lane.buffered_bytes += chunk.bytes();
    lane.buffer.push_back(std::move(chunk));
    if (!lane.underCaps(max_rows_to_buffer, max_bytes_to_buffer))
        invalidateSet();

    /// A merge never needs more than `limit` rows from one source.
    if (limit && lane.rows_pulled >= limit)
        lane.input->close();
}

void VirtualRowReadAheadTransform::setBound(size_t lane_num, Columns key, bool from_virtual_row)
{
    Lane & lane = lanes[lane_num];
    /// The order of `ranked_lanes` reads the bound, so the lane is out of it while the bound changes.
    if (lane.ranked)
        ranked_lanes.erase(lane_num);

    lane.bound = std::move(key);
    lane.bound_from_virtual_row = from_virtual_row;

    lane.ranked = !lane.output_finished;
    if (lane.ranked)
        ranked_lanes.insert(lane_num);

    /// A member that stays below the frontier changes neither the set nor the frontier; a lane
    /// outside the set may be the frontier itself, and a member past it has to leave.
    if (!lane.in_set || passedFrontier(lane_num))
        invalidateSet();
}

/// The K - 1 lanes with the smallest bounds other than the demanded one may each pull one
/// block of data, so that the merge finds it buffered when the demanded lane runs out.
void VirtualRowReadAheadTransform::grantWarmup(size_t lane_num)
{
    if (read_ahead_window <= 1 || (limit && budget_rows >= limit))
        return;

    size_t granted = 0;
    for (size_t other_num : ranked_lanes)
    {
        if (granted + 1 >= read_ahead_window)
            break;
        if (other_num == lane_num)
            continue;

        Lane & other = lanes[other_num];
        if (other.output_finished || other.input->isFinished())
            continue;

        ++granted;
        /// A lane that already holds a block keeps its place in the count but pulls no more.
        if (other.buffered_rows > 0 || other.warmup_credit)
            continue;

        other.warmup_credit = true;
        enqueue(other_num);
    }
}

void VirtualRowReadAheadTransform::finishLane(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.output_finished)
        return;

    lane.output_finished = true;
    ++finished_outputs;
    lane.output->finish();
    lane.input->close();

    lane.buffer.clear();
    lane.buffered_rows = 0;
    lane.buffered_bytes = 0;
    lane.warmup_credit = false;
    lane.in_set = false;

    if (lane.ranked)
    {
        ranked_lanes.erase(lane_num);
        lane.ranked = false;
    }

    budget_rows -= lane.rows_pulled;
    if (last_demanded_lane == static_cast<ssize_t>(lane_num))
        last_demanded_lane = -1;
    invalidateSet();
}

IProcessor::Status VirtualRowReadAheadTransform::prepare()
{
    UpdatedInputPorts all_inputs;
    UpdatedOutputPorts all_outputs;
    all_inputs.reserve(inputs.size());
    all_outputs.reserve(outputs.size());
    for (auto & input : inputs)
        all_inputs.push_back(&input);
    for (auto & output : outputs)
        all_outputs.push_back(&output);
    return prepareImpl(all_inputs, all_outputs);
}

IProcessor::Status VirtualRowReadAheadTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    return prepareImpl(updated_inputs, updated_outputs);
}

IProcessor::Status VirtualRowReadAheadTransform::prepareImpl(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;
        for (size_t lane_num = 0; lane_num < lanes.size(); ++lane_num)
            enqueue(lane_num);
    }

    for (const auto * output : updated_outputs)
        enqueue(lane_by_port.at(output));
    for (const auto * input : updated_inputs)
        enqueue(lane_by_port.at(input));

    runLanes();

    if (finished_outputs == lanes.size())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    return Status::NeedData;
}

}
