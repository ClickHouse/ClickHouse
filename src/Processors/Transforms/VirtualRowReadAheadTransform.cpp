#include <Processors/Transforms/VirtualRowReadAheadTransform.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
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

/// ── Keys ─────────────────────────────────────────────────────────────────────────────────────

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

/// ── Policy: who may read ─────────────────────────────────────────────────────────────────────

/// The frontier as `lane_num` sees it. A lane reading outside the set (the one the merge is
/// draining, or any demanded lane when K = 0) may itself be the smallest-bound outsider and
/// must not be stopped by its own bound: it takes the next one in order.
ssize_t VirtualRowReadAheadTransform::frontierFor(size_t lane_num) const
{
    if (frontier_lane != static_cast<ssize_t>(lane_num))
        return frontier_lane;

    auto it = ranked_lanes.find(lane_num);
    for (++it; it != ranked_lanes.end(); ++it)
        if (!lanes[*it].in_set && !lanes[*it].exhausted)
            return *it;
    return -1;
}

bool VirtualRowReadAheadTransform::passedFrontier(size_t lane_num) const
{
    ssize_t frontier = frontierFor(lane_num);
    return lanes[lane_num].ranked() && frontier >= 0 && BoundLess{this}(static_cast<size_t>(frontier), lane_num);
}

/// Demand and a warm-up grant always read. Otherwise only a set member with the window open, or
/// the lane the merge is draining, reads, and it stops when the budget is spent, at its caps,
/// or once its bound passes the frontier.
bool VirtualRowReadAheadTransform::mayRead(size_t lane_num) const
{
    const Lane & lane = lanes[lane_num];
    if (lane.isDemanded() || lane.warmup)
        return true;

    bool active = (window_open && lane.in_set) || demanded_lane == static_cast<ssize_t>(lane_num);
    return active && !budgetSpent() && lane.underCaps(max_rows_to_buffer, max_bytes_to_buffer) && !passedFrontier(lane_num);
}

/// Walks the lanes in bound order: the first K that can read now, i.e. under their caps, are
/// the set, and the first one passed over, or the (K + 1)-th, is the frontier. A lane whose
/// input is exhausted is neither: the merge drains its buffer without any further read, so data
/// beyond its bound is premature for no one. Returns whether the readers, or the window, changed.
bool VirtualRowReadAheadTransform::chooseReaders()
{
    std::vector<size_t> next;
    ssize_t frontier = -1;
    for (size_t lane_num : ranked_lanes)
    {
        const Lane & lane = lanes[lane_num];
        if (lane.exhausted)
            continue;

        if (next.size() < read_ahead_window && lane.underCaps(max_rows_to_buffer, max_bytes_to_buffer))
            next.push_back(lane_num);
        else if (frontier < 0)
            frontier = lane_num;

        if (frontier >= 0 && next.size() == read_ahead_window)
            break;
    }

    for (size_t lane_num : readers)
        lanes[lane_num].in_set = false;
    for (size_t lane_num : next)
        lanes[lane_num].in_set = true;

    if (demanded_lane >= 0)
        next.push_back(demanded_lane);
    std::sort(next.begin(), next.end());
    next.erase(std::unique(next.begin(), next.end()), next.end());

    bool changed = next != readers || window_open != window_open_when_chosen;
    readers = std::move(next);
    frontier_lane = frontier;
    window_open_when_chosen = window_open;
    return changed;
}

/// ── Mechanics: moving chunks ─────────────────────────────────────────────────────────────────

/// Moves one lane as far as it can go right now: pushes what the merge will take, pulls while
/// the lane may read and data is there, and leaves the input needed only if the lane may still
/// read (a port left needed lets the source read one block into it).
void VirtualRowReadAheadTransform::serve(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.finished)
        return;

    if (lane.output->isFinished())
    {
        finishLane(lane_num);
        return;
    }

    pushReady(lane);
    if (lane.isDemanded())
        noteDemand(lane_num);

    while (!lane.input->isFinished() && mayRead(lane_num))
    {
        lane.input->setNeeded();
        if (!lane.input->hasData())
            return;

        consume(lane_num, lane.input->pull(/* set_not_needed */ true));
        pushReady(lane);
    }

    lane.exhausted = lane.input->isFinished();
    if (!lane.exhausted)
        lane.input->setNotNeeded();
    else if (lane.buffer.empty())
        finishLane(lane_num);
}

void VirtualRowReadAheadTransform::pushReady(Lane & lane)
{
    while (!lane.buffer.empty() && lane.output->canPush())
    {
        Chunk chunk = std::move(lane.buffer.front());
        lane.buffer.pop_front();
        if (!isVirtualRow(chunk))
        {
            lane.buffered_rows -= chunk.getNumRows();
            lane.buffered_bytes -= chunk.bytes();
        }
        lane.output->push(std::move(chunk));
    }
}

void VirtualRowReadAheadTransform::consume(size_t lane_num, Chunk chunk)
{
    Lane & lane = lanes[lane_num];

    if (isVirtualRow(chunk))
    {
        /// Two announcements in a row: the block between them was fully filtered.
        bool filtered_stretch = lane.bound_is_virtual_row;
        setBound(lane_num, virtualRowKey(chunk), /* is_virtual_row */ true);

        /// The merge needs only the latest announcement.
        if (lane.holdsVirtualRow())
            lane.buffer.back() = std::move(chunk);
        else
            lane.buffer.push_back(std::move(chunk));

        if (filtered_stretch && limit && !window_open && demanded_lane == static_cast<ssize_t>(lane_num))
            grantWarmup(lane_num);
        return;
    }

    /// The warm-up grant is one block of data. A source never pushes a fully filtered block,
    /// only the virtual row after it, so the grant lasts through a filtered stretch: it is meant
    /// to leave the lane's first block buffered for the merge, and the filtered blocks pull no
    /// rows, which is the currency the waste bound is stated in.
    lane.warmup = false;

    if (!chunk.hasRows())
        return;

    size_t rows = chunk.getNumRows();
    setBound(lane_num, lastRowKey(chunk), /* is_virtual_row */ false);
    lane.rows_pulled += rows;
    budget_rows += rows;

    /// Data supersedes the announcement ahead of it.
    if (lane.holdsVirtualRow())
        lane.buffer.pop_back();
    lane.buffered_rows += rows;
    lane.buffered_bytes += chunk.bytes();
    lane.buffer.push_back(std::move(chunk));

    /// A merge never needs more than `limit` rows from one source.
    if (limit && lane.rows_pulled >= limit)
        lane.input->close();
}

void VirtualRowReadAheadTransform::setBound(size_t lane_num, Columns key, bool is_virtual_row)
{
    Lane & lane = lanes[lane_num];
    /// The order of `ranked_lanes` reads the bound, so the lane is out of it while the bound changes.
    if (lane.ranked())
        ranked_lanes.erase(lane_num);
    lane.bound = std::move(key);
    lane.bound_is_virtual_row = is_virtual_row;
    ranked_lanes.insert(lane_num);
}

/// With a limit the set may read only once the merge has moved on to a second lane parked
/// behind a virtual row; until then everything it does not ask for stays unread. The lane asked
/// for last keeps reading ahead on its own; the one before it loses that right.
void VirtualRowReadAheadTransform::noteDemand(size_t lane_num)
{
    const Lane & lane = lanes[lane_num];
    /// The merge's initialisation asks every lane before any bound is known; those requests are
    /// served with the first chunk and grant nothing beyond it.
    if (!lane.ranked())
        return;

    if (limit && !window_open && lane.bound_is_virtual_row)
    {
        if (first_demanded_lane < 0)
            first_demanded_lane = lane_num;
        else if (first_demanded_lane != static_cast<ssize_t>(lane_num))
            window_open = true;
    }

    demanded_lane = lane_num;
}

/// The K - 1 lanes with the smallest bounds other than the demanded one may each pull one
/// block of data, so that the merge finds it buffered when the demanded lane runs out.
void VirtualRowReadAheadTransform::grantWarmup(size_t lane_num)
{
    if (read_ahead_window <= 1 || budgetSpent())
        return;

    std::vector<size_t> granted;
    for (size_t other_num : ranked_lanes)
    {
        if (granted.size() + 1 >= read_ahead_window)
            break;
        const Lane & other = lanes[other_num];
        if (other_num == lane_num || other.exhausted)
            continue;
        /// A lane that already holds a block keeps its place in the count but pulls no more.
        if (other.buffered_rows == 0 && !other.warmup)
            lanes[other_num].warmup = true;
        granted.push_back(other_num);
    }

    /// Served after the walk: reading moves a lane in `ranked_lanes`.
    for (size_t other_num : granted)
        serve(other_num);
}

void VirtualRowReadAheadTransform::finishLane(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.ranked())
        ranked_lanes.erase(lane_num);
    lane.finished = true;
    ++finished_lanes;

    lane.output->finish();
    lane.input->close();
    lane.buffer.clear();
    lane.buffered_rows = 0;
    lane.buffered_bytes = 0;
    lane.in_set = false;

    budget_rows -= lane.rows_pulled;
    if (demanded_lane == static_cast<ssize_t>(lane_num))
        demanded_lane = -1;
}

/// ── Entry point ──────────────────────────────────────────────────────────────────────────────

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
            serve(lane_num);
    }
    for (const auto * output : updated_outputs)
        serve(lane_by_port.at(output));
    for (const auto * input : updated_inputs)
        serve(lane_by_port.at(input));

    /// Serving a lane can change who reads ahead: a bound moved, a lane finished or filled its
    /// buffer, the window opened. The executor does not call back for that, so the readers are
    /// served again here until they stand still.
    while (chooseReaders())
        for (size_t lane_num : readers)
            serve(lane_num);

    if (finished_lanes == lanes.size())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }
    return Status::NeedData;
}

}
