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

bool VirtualRowReadAheadTransform::underCaps(const Lane & lane) const
{
    return (max_rows_to_buffer == 0 || lane.buffered_rows < max_rows_to_buffer)
        && (max_bytes_to_buffer == 0 || lane.buffered_bytes < max_bytes_to_buffer);
}

bool VirtualRowReadAheadTransform::isDemanded(const Lane & lane) const
{
    return lane.buffer.empty() && lane.output->canPush();
}

ssize_t VirtualRowReadAheadTransform::frontierFor(size_t lane_num) const
{
    if (frontier_lanes[0] == static_cast<ssize_t>(lane_num))
        return frontier_lanes[1];
    return frontier_lanes[0];
}

bool VirtualRowReadAheadTransform::mayRead(size_t lane_num) const
{
    const Lane & lane = lanes[lane_num];
    if (lane.output_finished || lane.input->isFinished())
        return false;

    if (isDemanded(lane))
        return true;

    if (lane.warmup_credit)
        return true;

    /// A demanded lane keeps the rights of a set member after the chunk that served the demand.
    bool active = (window_open && lane.in_set) || last_demanded_lane == static_cast<ssize_t>(lane_num);
    if (!active)
        return false;

    if (limit && budget_rows >= limit)
        return false;

    if (!underCaps(lane))
        return false;

    ssize_t frontier = frontierFor(lane_num);
    if (lane.ranked && frontier >= 0 && BoundLess{this}(static_cast<size_t>(frontier), lane_num))
        return false;

    return true;
}

void VirtualRowReadAheadTransform::enqueue(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.queued)
        return;
    lane.queued = true;
    candidates.push_back(lane_num);
}

void VirtualRowReadAheadTransform::recomputeSet()
{
    std::swap(previous_set_lanes, set_lanes);
    for (size_t lane_num : previous_set_lanes)
        lanes[lane_num].in_set = false;

    set_lanes.clear();
    frontier_lanes[0] = frontier_lanes[1] = -1;
    size_t num_frontier = 0;

    for (size_t lane_num : ranked_lanes)
    {
        Lane & lane = lanes[lane_num];
        /// A lane with nothing left to read neither takes a slot nor bounds the others: the merge
        /// will drain its buffer without any further read, so data beyond its bound is not premature.
        if (lane.input->isFinished())
            continue;

        bool eligible = set_lanes.size() < read_ahead_window && underCaps(lane);
        if (eligible)
        {
            lane.in_set = true;
            set_lanes.push_back(lane_num);
        }
        else if (num_frontier < 2)
        {
            frontier_lanes[num_frontier++] = lane_num;
        }

        if (set_lanes.size() == read_ahead_window && num_frontier == 2)
            break;
    }

    /// Members that left must release their input, members and newcomers must re-check the frontier and the budget.
    for (size_t lane_num : previous_set_lanes)
        enqueue(lane_num);
    for (size_t lane_num : set_lanes)
        enqueue(lane_num);
}

void VirtualRowReadAheadTransform::runLanes()
{
    /// Every pass applies the consequences of the previous one (a lane entering or leaving the
    /// set, the frontier or the budget moving, the window opening) to the lanes concerned, so a
    /// single call settles the whole processor. Each flag raise is a state transition of finite
    /// supply (a pulled or pushed chunk, a finished lane, the window opening), so this ends.
    while (true)
    {
        if (recompute_needed)
        {
            recompute_needed = false;
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

        if (!recompute_needed)
            return;
    }
}

void VirtualRowReadAheadTransform::pushFromBuffer(Lane & lane)
{
    while (!lane.buffer.empty() && lane.output->canPush())
    {
        bool is_virtual_row = lane.pending_virtual_row && lane.buffer.size() == 1;
        Chunk chunk = std::move(lane.buffer.front());
        lane.buffer.pop_front();

        if (is_virtual_row)
        {
            lane.pending_virtual_row = false;
        }
        else
        {
            lane.buffered_rows -= chunk.getNumRows();
            lane.buffered_bytes -= chunk.bytes();
            /// The lane may be under its caps again.
            recompute_needed = true;
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

    if (limit && !window_open && lane.bound_from_virtual_row)
    {
        if (first_demanded_lane < 0)
        {
            first_demanded_lane = lane_num;
        }
        else if (first_demanded_lane != static_cast<ssize_t>(lane_num))
        {
            window_open = true;
            recompute_needed = true;
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

    if (lane.input->isFinished())
    {
        noteInputFinished(lane_num);
        return;
    }

    if (isDemanded(lane))
        noteDemand(lane_num);

    while (true)
    {
        if (!mayRead(lane_num))
        {
            lane.input->setNotNeeded();
            break;
        }

        lane.input->setNeeded();
        if (!lane.input->hasData())
            break;

        consume(lane_num, lane.input->pull(/* set_not_needed */ true));
        pushFromBuffer(lane);

        if (lane.input->isFinished())
        {
            noteInputFinished(lane_num);
            break;
        }
    }
}

void VirtualRowReadAheadTransform::noteInputFinished(size_t lane_num)
{
    Lane & lane = lanes[lane_num];
    if (lane.buffer.empty())
    {
        finishLane(lane_num);
        return;
    }

    /// Nothing more to read: the lane leaves the set while the merge drains its buffer.
    if (!lane.input_finished_noted)
    {
        lane.input_finished_noted = true;
        recompute_needed = true;
    }
}

void VirtualRowReadAheadTransform::consume(size_t lane_num, Chunk chunk)
{
    Lane & lane = lanes[lane_num];

    if (isVirtualRow(chunk))
    {
        bool filtered_stretch = lane.announced && lane.rows_since_announcement == 0;
        lane.announced = true;
        lane.rows_since_announcement = 0;
        setBound(lane_num, virtualRowKey(chunk), /* from_virtual_row */ true);

        if (lane.pending_virtual_row)
        {
            lane.buffer.back() = std::move(chunk);
        }
        else
        {
            lane.buffer.push_back(std::move(chunk));
            lane.pending_virtual_row = true;
        }

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
    lane.rows_since_announcement += rows;
    budget_rows += rows;

    if (lane.pending_virtual_row)
    {
        lane.buffer.pop_back();
        lane.pending_virtual_row = false;
    }

    lane.buffered_rows += rows;
    lane.buffered_bytes += chunk.bytes();
    lane.buffer.push_back(std::move(chunk));

    /// A merge never needs more than `limit` rows from one source.
    if (limit && lane.rows_pulled >= limit)
        lane.input->close();
}

void VirtualRowReadAheadTransform::setBound(size_t lane_num, Columns key, bool from_virtual_row)
{
    Lane & lane = lanes[lane_num];
    if (lane.ranked)
    {
        ranked_lanes.erase(lane.rank_it);
        lane.ranked = false;
    }

    lane.bound = std::move(key);
    lane.bound_from_virtual_row = from_virtual_row;
    recompute_needed = true;

    if (!lane.output_finished)
    {
        lane.rank_it = ranked_lanes.insert(lane_num).first;
        lane.ranked = true;
    }
}

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
    lane.pending_virtual_row = false;
    lane.buffered_rows = 0;
    lane.buffered_bytes = 0;
    lane.warmup_credit = false;
    lane.in_set = false;

    if (lane.ranked)
    {
        ranked_lanes.erase(lane.rank_it);
        lane.ranked = false;
    }

    budget_rows -= lane.rows_pulled;
    if (last_demanded_lane == static_cast<ssize_t>(lane_num))
        last_demanded_lane = -1;
    recompute_needed = true;
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
