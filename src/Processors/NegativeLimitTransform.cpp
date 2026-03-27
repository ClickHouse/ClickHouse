#include <Columns/IColumn.h>
#include <Processors/NegativeLimitTransform.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

NegativeLimitTransform::NegativeLimitTransform(
    SharedHeader header_, UInt64 limit_, UInt64 offset_, size_t num_streams, bool with_ties_, SortDescription description_)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , limit(limit_)
    , offset(offset_)
    , with_ties(with_ties_ && limit_ != 0)
    , description(std::move(description_))
{
    if (num_streams != 1 && with_ties)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot use NegativeLimitTransform with multiple ports and ties");

    if (with_ties && description.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "NegativeLimitTransform WITH TIES requires a non-empty SortDescription");

    ports_data.resize(num_streams);

    size_t cur_stream = 0;
    for (auto & input : inputs)
    {
        ports_data[cur_stream].input_port = &input;
        ++cur_stream;
    }

    cur_stream = 0;
    for (auto & output : outputs)
    {
        ports_data[cur_stream].output_port = &output;
        ++cur_stream;
    }

    for (const auto & desc : description)
        sort_column_positions.push_back(header_->getPositionByName(desc.column_name));
}

/// First, our goal is to pull all the data from input ports. Once we have reached the end,
/// then it is clear what should be part of the `limit`, `offset` and what should be pushed out to the output ports.
NegativeLimitTransform::Status NegativeLimitTransform::prepare()
{
    if (allOutputsFinished())
    {
        for (auto & port : ports_data)
            port.input_port->close();
        return Status::Finished;
    }

    if (stage == Stage::Pull)
    {
        bool has_data_need = false;

        auto process = [&](size_t pos)
        {
            auto status = advancePort(pos);
            switch (status)
            {
                case IProcessor::Status::Finished: {
                    if (!ports_data[pos].is_input_port_finished)
                    {
                        ports_data[pos].is_input_port_finished = true;
                        ++num_input_ports_finished;
                    }
                    return;
                }
                case IProcessor::Status::NeedData: {
                    has_data_need = true;
                    return;
                }
                default:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected status in NegativeLimitTransform::advancePort : {}",
                        IProcessor::statusToName(status));
            }
        };

        for (size_t pos = 0; pos < ports_data.size(); ++pos)
            process(pos);

        if (has_data_need)
            return Status::NeedData;

        /// All data fetching is done
        if (num_input_ports_finished == ports_data.size())
        {
            if (with_ties)
            {
                /// The boundary and tie-run start are already tracked.
                /// Cut the rows before the tie-run in the front chunk so queued_row_count
                /// reflects exactly the output window + offset tail. Output window contains limit window + tie-run.
                cutFrontChunkToRunStart();
                assertRowCountConsistency();

                /// `boundary.has_excess` is true only when the total data exceeded
                /// limit + offset during Pull, i.e. there was actual excess to discard.
                /// When false (not enough data), the assertion is vacuously true.
                /// When true, ties can only extend the window, so output >= `limit`.
                chassert(!boundary.has_excess || (queued_row_count > offset && queued_row_count - offset >= limit));
            }

            stage = Stage::Push;
        }
        else
        {
            return Status::NeedData;
        }
    }

    if (stage == Stage::Push)
    {
        if (with_ties)
        {
            /// After cutFrontChunkToRunStart, the deque contains exactly the
            /// output window (tie-run + limit window) followed by the offset tail.
            /// Push queued_row_count - offset rows.
            UInt64 to_push = queued_row_count > offset ? queued_row_count - offset : 0;
            if (to_push == 0)
            {
                for (auto & port : ports_data)
                {
                    port.input_port->close();
                    port.output_port->finish();
                }
                return Status::Finished;
            }

            /// Push front chunks consisting of `to_push` amount of rows to output ports, cutting the
            /// last chunk if it extends into the offset tail. Returns PortFull if no output can
            /// accept data yet; we will be re-entered on the next `prepare` call.
            Status status = pushRows(to_push);
            if (status != Status::Finished)
                return status;

            for (auto & port : ports_data)
            {
                port.input_port->close();
                port.output_port->finish();
            }
            return Status::Finished;
        }

        /// If we enter this stage, it means that we have all the input data and all input ports are closed, and there
        /// are three scenarios:
        /// 1. queued_row_count > limit + offset
        ///    I. We first get rid of the prefix of leftmost chunk to make queued_row_count == limit + offset.
        ///    II. Then keep pushing the left whole chunks to output ports without going into the offset area.
        ///    III. Finally, get rid of the suffix of the leftmost chunk to make queued_row_count == offset, and push the
        ///         cut chunk to output port.
        ///
        /// 2. queued_row_count > offset but <= limit + offset (if there are fewer than limit + offset rows in total)
        ///    I. Follow step II of scenario 1
        ///    II. Follow step III of scenario 1
        ///
        /// 3. queued_row_count <= offset  (if there are no more than offset rows in total)
        ///     I. Nothing to do or push. Close the output ports.

        // (queued_row_count > limit + offset) in an overflow-safe way
        if (queued_row_count > offset && queued_row_count - offset > limit)
        {
            /// Step I: cut the front chunk's prefix that is before limit + offset.
            chassert(!chunks.empty());
            auto & chunk = chunks.front();
            const UInt64 front_rows = chunk.getNumRows();
            const UInt64 start = (queued_row_count - limit) - offset;
            chassert(start < front_rows);
            const UInt64 take = front_rows - start;

            auto columns = chunk.detachColumns();
            for (auto & col : columns)
                col = col->cut(start, take);
            chunk.setColumns(std::move(columns), take);
            queued_row_count -= (front_rows - take);
        }

        /// Steps II + III: push queued_row_count - offset rows.
        {
            UInt64 to_push = queued_row_count > offset ? queued_row_count - offset : 0;
            if (to_push > 0)
            {
                Status status = pushRows(to_push);
                if (status != Status::Finished)
                    return status;
            }
        }

        chassert(queued_row_count <= offset);

        for (auto & port : ports_data)
        {
            port.input_port->close();
            port.output_port->finish();
        }
        return Status::Finished;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "NegativeLimitTransform::prepare in unknown stage");
}

bool NegativeLimitTransform::allOutputsFinished() const
{
    for (const auto & data : ports_data)
    {
        if (!data.output_port->isFinished())
            return false;
    }
    return true;
}

OutputPort * NegativeLimitTransform::getAvailableOutputPort()
{
    const size_t num_outputs = ports_data.size();

    if (num_outputs == 0)
        return nullptr;

    for (size_t i = 0; i < num_outputs; ++i)
    {
        const size_t idx = (next_output_port + i) % num_outputs;

        auto & output = *ports_data[idx].output_port;
        if (output.isFinished())
            continue;

        if (!output.canPush())
            continue;

        next_output_port = (idx + 1) % num_outputs;
        return &output;
    }

    return nullptr;
}

bool NegativeLimitTransform::sortKeysEqual(const Chunk & lhs, UInt64 lhs_row, const Chunk & rhs, UInt64 rhs_row) const
{
    const auto & lhs_cols = lhs.getColumns();
    const auto & rhs_cols = rhs.getColumns();
    for (size_t i = 0; i < sort_column_positions.size(); ++i)
    {
        const size_t pos = sort_column_positions[i];
        const auto & desc = description[i];

        if (lhs_cols[pos]->compareAt(lhs_row, rhs_row, *rhs_cols[pos], desc.nulls_direction) != 0)
            return false;
    }
    return true;
}

NegativeLimitTransform::Status NegativeLimitTransform::advancePort(size_t pos)
{
    auto & data = ports_data[pos];
    auto & input = *data.input_port;

    /// Check if input is available.
    if (input.isFinished())
    {
        return Status::Finished;
    }

    input.setNeeded();

    if (input.hasData())
    {
        Chunk chunk = input.pull(true);

        input.setNeeded();

        auto rows = chunk.getNumRows();
        if (rows == 0)
            return input.isFinished() ? Status::Finished : Status::NeedData;

        queued_row_count += rows;

        if (rows_before_limit_at_least && !data.input_port_has_counter)
        {
            rows_before_limit_at_least->add(rows);
        }

        chunks.push_back(std::move(chunk));

        if (!with_ties)
        {
            /// Try removing the whole chunks that will never be part of the LIMIT.
            /// In short, we are checking if (queued_row_count - front_chunk_rows) >= offset + limit.
            /// It is written this way to avoid potential overflow.
            while (!chunks.empty())
            {
                const UInt64 front_rows = chunks.front().getNumRows();
                const UInt64 rem = queued_row_count - front_rows;
                if (rem >= offset && (rem - offset) >= limit)
                {
                    queued_row_count -= front_rows;
                    chunks.pop_front();
                }
                else
                    break;
            }
            assertRowCountConsistency();
        }
        else /// WITH TIES
        {
            /// We cannot simply discard chunks before the limit window
            /// because the output window extends backward to include the tie-run.
            /// Instead, we track the boundary (first row of the limit window) and
            /// the earliest chunk in its tie-run, discarding only chunks before that.
            /// We keep track of these states to keep memory usage as low as possible:
            /// chunks at the front of the deque that we no longer need are discarded eagerly.
            if (!hasExcessBeforeLimitWindow())
            {
                /// All data so far fits within the window — nothing to discard.
                boundary.has_excess = false;
            }
            else if (!boundary.has_excess) /// This branch runs at most once
            {
                /// First time we have excess rows. Initialize the boundary.
                boundary.has_excess = true;

                UInt64 target = boundaryPosFromFront();
                UInt64 cumulative = 0;

                /// Find the chunk containing the boundary row and the row index within that chunk.
                boundary.limit_boundary_chunk_idx = 0;
                for (; boundary.limit_boundary_chunk_idx < chunks.size(); ++boundary.limit_boundary_chunk_idx)
                {
                    UInt64 chunk_rows = chunks[boundary.limit_boundary_chunk_idx].getNumRows();
                    if (cumulative + chunk_rows > target)
                        break;
                    cumulative += chunk_rows;
                }
                /// Now set the boundary row index within the chunk.
                boundary.limit_boundary_row_idx = target - cumulative;

                chassert(boundary.limit_boundary_chunk_idx < chunks.size());
                chassert(boundary.limit_boundary_row_idx < chunks[boundary.limit_boundary_chunk_idx].getNumRows());

                findRunStartChunk();
                chassert(boundary.tie_run_start_chunk_idx <= boundary.limit_boundary_chunk_idx);
                discardChunksBeforeRunStart();
                chassert(boundary.tie_run_start_chunk_idx == 0);
                assertRowCountConsistency();
            }
            else
            {
                /// Boundary was already initialized. Appending `rows` rows moves
                /// the boundary right by exactly `rows`.
                const auto & old_boundary_chunk = chunks[boundary.limit_boundary_chunk_idx];
                UInt64 old_row_idx = boundary.limit_boundary_row_idx;

                advanceLimitBoundary(rows);
                chassert(boundary.limit_boundary_chunk_idx < chunks.size());
                chassert(boundary.limit_boundary_row_idx < chunks[boundary.limit_boundary_chunk_idx].getNumRows());

                if (!sortKeysEqual(
                        old_boundary_chunk, old_row_idx, chunks[boundary.limit_boundary_chunk_idx], boundary.limit_boundary_row_idx))
                {
                    findRunStartChunk();
                    chassert(boundary.tie_run_start_chunk_idx <= boundary.limit_boundary_chunk_idx);
                }

                discardChunksBeforeRunStart();
                chassert(boundary.tie_run_start_chunk_idx == 0);
                assertRowCountConsistency();
            }
        }
    }

    if (input.isFinished())
        return Status::Finished;

    return Status::NeedData;
}


void NegativeLimitTransform::advanceLimitBoundary(UInt64 delta)
{
    while (delta > 0)
    {
        const UInt64 chunk_rows = chunks[boundary.limit_boundary_chunk_idx].getNumRows();
        const UInt64 remaining_in_chunk = chunk_rows - boundary.limit_boundary_row_idx - 1;

        if (delta <= remaining_in_chunk)
        {
            boundary.limit_boundary_row_idx += delta;
            return;
        }

        delta -= remaining_in_chunk + 1;
        ++boundary.limit_boundary_chunk_idx;
        boundary.limit_boundary_row_idx = 0;
    }
}

void NegativeLimitTransform::findRunStartChunk()
{
    const auto & boundary_chunk = chunks[boundary.limit_boundary_chunk_idx];

    /// Scan leftward from the boundary chunk. For each chunk, check whether
    /// its first row matches the boundary key (meaning the whole chunk is tied,
    /// since data is sorted). Stop when we find a chunk whose first row differs,
    /// or when we reach the beginning of the deque.
    boundary.tie_run_start_chunk_idx = boundary.limit_boundary_chunk_idx;
    while (boundary.tie_run_start_chunk_idx > 0)
    {
        const auto & cur_chunk = chunks[boundary.tie_run_start_chunk_idx];

        /// First row differs from boundary key — tie-run starts inside this chunk.
        if (!sortKeysEqual(cur_chunk, 0, boundary_chunk, boundary.limit_boundary_row_idx))
            break;

        const auto & prev_chunk = chunks[boundary.tie_run_start_chunk_idx - 1];

        /// Previous chunk's last row differs — tie-run doesn't extend further left.
        if (!sortKeysEqual(prev_chunk, prev_chunk.getNumRows() - 1, boundary_chunk, boundary.limit_boundary_row_idx))
            break;

        --boundary.tie_run_start_chunk_idx;
    }
}

void NegativeLimitTransform::discardChunksBeforeRunStart()
{
    while (boundary.tie_run_start_chunk_idx > 0)
    {
        queued_row_count -= chunks.front().getNumRows();
        chunks.pop_front();

        --boundary.limit_boundary_chunk_idx;
        --boundary.tie_run_start_chunk_idx;
    }
}

void NegativeLimitTransform::cutFrontChunkToRunStart()
{
    chassert(with_ties);
    if (chunks.empty())
        return;

    if (!hasExcessBeforeLimitWindow())
        return; /// All data fits — no excess to cut.

    chassert(boundary.has_excess);

    /// The run start chunk may contain rows before the tie-run start.
    /// Binary search for the first row equal to the boundary key.
    chassert(boundary.tie_run_start_chunk_idx == 0);
    auto & run_start_chunk = chunks[boundary.tie_run_start_chunk_idx];
    const auto & boundary_chunk = chunks[boundary.limit_boundary_chunk_idx];

    /// If the run start chunk's first row already matches the boundary key,
    /// there's nothing to cut.
    if (sortKeysEqual(run_start_chunk, 0, boundary_chunk, boundary.limit_boundary_row_idx))
        return;

    /// Binary search for the first row equal to the boundary key.
    /// We search only within [0, hi) where hi is at or before the boundary
    /// position — all rows in this range are <= boundary in sort order,
    /// so non-equal rows are strictly before equal rows and the equality-based
    /// binary search is correct regardless of collation.
    UInt64 lo = 0;
    UInt64 hi = (boundary.limit_boundary_chunk_idx == 0) ? boundary.limit_boundary_row_idx + 1 : run_start_chunk.getNumRows();
    while (lo < hi)
    {
        UInt64 mid = lo + (hi - lo) / 2;
        if (!sortKeysEqual(run_start_chunk, mid, boundary_chunk, boundary.limit_boundary_row_idx))
            lo = mid + 1;
        else
            hi = mid;
    }

    chassert(lo < run_start_chunk.getNumRows());
    chassert(sortKeysEqual(run_start_chunk, lo, boundary_chunk, boundary.limit_boundary_row_idx));

    /// Cut away the prefix [0, lo).
    if (lo > 0)
    {
        const UInt64 old_rows = run_start_chunk.getNumRows();
        const UInt64 new_rows = old_rows - lo;
        auto columns = run_start_chunk.detachColumns();
        for (auto & col : columns)
            col = col->cut(lo, new_rows);
        run_start_chunk.setColumns(std::move(columns), new_rows);
        queued_row_count -= lo;

        /// Adjust boundary indices since we removed rows from the front chunk.
        if (boundary.limit_boundary_chunk_idx == 0)
            boundary.limit_boundary_row_idx -= lo;
    }
}

NegativeLimitTransform::Status NegativeLimitTransform::pushRows(UInt64 to_push)
{
    chassert(to_push <= queued_row_count);

    while (to_push > 0)
    {
        chassert(!chunks.empty());

        auto & chunk = chunks.front();
        const UInt64 front_rows = chunk.getNumRows();

        if (front_rows <= to_push)
        {
            /// Push the whole chunk.
            auto * output = getAvailableOutputPort();
            if (!output)
                return Status::PortFull;

            to_push -= front_rows;
            queued_row_count -= front_rows;
            output->push(std::move(chunk));
            chunks.pop_front();
        }
        else
        {
            /// Push only the prefix of the front chunk.
            auto * output = getAvailableOutputPort();
            if (!output)
                return Status::PortFull;

            auto columns = chunk.detachColumns();
            for (auto & col : columns)
                col = col->cut(0, to_push);
            chunk.setColumns(std::move(columns), to_push);

            queued_row_count -= front_rows;
            to_push = 0;
            output->push(std::move(chunk));
            chunks.pop_front();
        }
    }

    return Status::Finished;
}

}
