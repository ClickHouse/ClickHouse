#include <Processors/QueryPlan/BufferChunksTransform.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

namespace DB
{

ReadInOrderReadAheadTracker::ReadInOrderReadAheadTracker(
    size_t num_inputs, const Block & header, const SortDescription & description_, size_t limit_, bool merge_has_limit_)
    : description(description_)
    , limit(limit_)
    , merge_has_limit(merge_has_limit_)
    , inputs(num_inputs)
{
    sort_column_positions.reserve(description.size());
    for (const auto & column_description : description)
        sort_column_positions.push_back(header.getPositionByName(column_description.column_name));
}

Columns ReadInOrderReadAheadTracker::cutRow(const Chunk & chunk, size_t row) const
{
    const auto & columns = chunk.getColumns();
    Columns res;
    res.reserve(sort_column_positions.size());
    /// Without `LowCardinality`, so that one row does not keep the whole dictionary alive.
    for (size_t position : sort_column_positions)
        res.push_back(recursiveRemoveLowCardinality(columns[position]->cut(row, 1)->convertToFullIfWrapped()));
    return res;
}

void ReadInOrderReadAheadTracker::addChunk(size_t input, const Chunk & chunk, size_t rows)
{
    chassert(input < inputs.size() && chunk.getNumRows() > 0);
    auto & state = inputs[input];

    if (merge_has_limit)
    {
        state.produced.store(rows, std::memory_order_relaxed);
    }
    else
    {
        Columns last_row = cutRow(chunk, chunk.getNumRows() - 1);
        std::lock_guard lock(state.mutex);
        state.last_row.swap(last_row);
        state.produced.store(rows, std::memory_order_relaxed);
    }

    if (!state.first_row.empty())
        return;

    state.first_row = cutRow(chunk, 0);
    size_t slot = num_published.fetch_add(1, std::memory_order_relaxed);
    if (slot < published.size())
        published[slot].store(input + 1, std::memory_order_release);
}

int ReadInOrderReadAheadTracker::compareRows(const Columns & lhs, const Columns & rhs) const
{
    for (size_t i = 0; i < description.size(); ++i)
    {
        if (int res = description[i].direction * lhs[i]->compareAt(0, 0, *rhs[i], description[i].nulls_direction))
            return res;
    }
    return 0;
}

size_t ReadInOrderReadAheadTracker::rowsProducedBefore(size_t input, InputsBefore & inputs_before) const
{
    const auto & first_row = inputs[input].first_row;
    chassert(!first_row.empty());

    for (; inputs_before.num_compared < published.size(); ++inputs_before.num_compared)
    {
        size_t entry = published[inputs_before.num_compared].load(std::memory_order_acquire);
        if (entry == 0)
            break;

        if (compareRows(inputs[entry - 1].first_row, first_row) < 0)
            inputs_before.inputs.push_back({.input = entry - 1});
    }

    size_t rows = 0;
    for (auto & before : inputs_before.inputs)
    {
        const auto & state = inputs[before.input];
        if (merge_has_limit)
        {
            rows += state.produced.load(std::memory_order_relaxed);
            continue;
        }

        /// Once the other input reaches our first row, the rows counted until then still sort before ours.
        if (!before.overlaps && state.produced.load(std::memory_order_relaxed) != before.rows)
        {
            std::lock_guard lock(state.mutex);
            if (compareRows(state.last_row, first_row) < 0)
                before.rows = state.produced.load(std::memory_order_relaxed);
            else
                before.overlaps = true;
        }
        rows += before.rows;
    }
    return rows;
}

BufferChunksTransform::BufferChunksTransform(
    SharedHeader header_,
    size_t max_rows_to_buffer_,
    size_t max_bytes_to_buffer_,
    size_t limit_,
    std::shared_ptr<ReadInOrderReadAheadTracker> read_ahead_tracker_,
    size_t input_index_)
    : IProcessor({header_}, {header_})
    , input(inputs.front())
    , output(outputs.front())
    , max_rows_to_buffer(max_rows_to_buffer_)
    , max_bytes_to_buffer(max_bytes_to_buffer_)
    , limit(limit_)
    , read_ahead_tracker(std::move(read_ahead_tracker_))
    , input_index(input_index_)
{
}

IProcessor::Status BufferChunksTransform::prepare()
{
    if (output.isFinished())
    {
        chunks = {};
        num_buffered_virtual_rows = 0;
        input.close();
        return Status::Finished;
    }

    if (input.isFinished() && chunks.empty())
    {
        output.finish();
        return Status::Finished;
    }

    /// Do not read ahead while the downstream merge may have deferred this source
    /// after seeing its virtual row. Resume only when data is actually demanded —
    /// either right away (the source is not deferred) or when the merge releases
    /// the deferred source (see `IMergingTransformBase` and `topUpPrefetch`).
    if (wait_for_demand_after_virtual_row)
    {
        if (!output.canPush())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        wait_for_demand_after_virtual_row = false;
    }

    if (output.canPush())
    {
        if (output_has_our_chunk)
        {
            ++chunks_taken_by_merge;
            output_has_our_chunk = false;
        }

        if (num_buffered_virtual_rows == 0)
            input.setNeeded();

        if (!chunks.empty())
        {
            if (pushBufferedChunk())
                return Status::PortFull;
        }
        else if (input.hasData())
        {
            bool virtual_row = false;
            auto chunk = pullChunk(virtual_row);
            output.push(std::move(chunk));
            output_has_our_chunk = true;
            if (virtual_row)
            {
                wait_for_demand_after_virtual_row = true;
                input.setNotNeeded();
                return Status::PortFull;
            }
        }
    }

    /// A virtual row is still buffered behind earlier real chunks. Downstream has not observed
    /// the marker yet, so keep upstream stopped: otherwise it could push a real chunk past the
    /// boundary before the marker is forwarded. Either a chunk was pushed above, or the output
    /// is full, so the executor will wake us up when downstream consumes the output.
    if (num_buffered_virtual_rows > 0)
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Pausing is safe only while the output port is not ready: the merge taking our chunk
    /// or asking for data reschedules us.
    if (read_ahead_tracker && first_row_published && !output.canPush() && chunks_taken_by_merge <= 1)
    {
        size_t rows = read_ahead_tracker->rowsProducedBefore(input_index, inputs_before);
        /// A merge without a limit of its own can need all our rows, e.g. to fill a block from overlapping inputs.
        if (limit)
            rows += num_processed_rows;

        if (rows >= read_ahead_tracker->getLimit())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }
    }

    if (input.hasData() && (num_buffered_rows < max_rows_to_buffer || num_buffered_bytes < max_bytes_to_buffer))
    {
        bool virtual_row = false;
        auto chunk = pullChunk(virtual_row);
        if (virtual_row)
        {
            /// A virtual row announces the boundary of everything that follows it in this
            /// stream, so it must keep its place: it may never overtake chunks already
            /// buffered ahead of it, or the merge sees a boundary that the stream's own next
            /// rows violate (`Virtual row boundary violated in MergingSortedAlgorithm`).
            /// `output.canPush()` is not a safe proxy for "nothing is buffered": the
            /// downstream runs concurrently with this `prepare()` and can drain the output
            /// port right after the push above, while `chunks` still holds earlier data.
            /// So always queue the marker, and let the drain below emit it in order.
            num_buffered_rows += chunk.getNumRows();
            num_buffered_bytes += chunk.bytes();
            ++num_buffered_virtual_rows;
            chunks.push(std::move(chunk));

            /// Downstream has not observed the marker yet, so upstream must not push real
            /// chunks past the boundary before it is forwarded.
            input.setNotNeeded();

            /// Emit the head of the queue right away when the output has room. This keeps the
            /// marker as prompt as a direct push whenever it is the only buffered chunk, and
            /// it also guarantees progress: returning `PortFull` without having pushed
            /// anything would leave the executor with no port state change to wake us on.
            if (output.canPush())
                pushBufferedChunk();

            return Status::PortFull;
        }
        compactReplicatedColumns(chunk);
        num_buffered_rows += chunk.getNumRows();
        num_buffered_bytes += chunk.bytes();
        chunks.push(std::move(chunk));
    }

    if (num_buffered_rows >= max_rows_to_buffer && num_buffered_bytes >= max_bytes_to_buffer)
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    input.setNeeded();
    return Status::NeedData;
}

bool BufferChunksTransform::pushBufferedChunk()
{
    auto chunk = std::move(chunks.front());
    chunks.pop();

    num_buffered_rows -= chunk.getNumRows();
    num_buffered_bytes -= chunk.bytes();

    const bool virtual_row = isVirtualRow(chunk);
    output.push(std::move(chunk));
    output_has_our_chunk = true;

    if (virtual_row)
    {
        --num_buffered_virtual_rows;

        /// Stop reading until downstream has consumed the virtual-row marker, otherwise we
        /// would pull real chunks past the part boundary and defeat the LIMIT/read-in-order
        /// optimizations.
        wait_for_demand_after_virtual_row = true;
        input.setNotNeeded();
    }

    return virtual_row;
}

Chunk BufferChunksTransform::pullChunk(bool & virtual_row)
{
    auto chunk = input.pull();
    virtual_row = isVirtualRow(chunk);
    if (!virtual_row)
    {
        num_processed_rows += chunk.getNumRows();
        if (read_ahead_tracker && chunk.getNumRows() > 0)
        {
            read_ahead_tracker->addChunk(input_index, chunk, num_processed_rows);
            first_row_published = true;
        }
    }

    if (limit && num_processed_rows >= limit)
        input.close();

    return chunk;
}

}
