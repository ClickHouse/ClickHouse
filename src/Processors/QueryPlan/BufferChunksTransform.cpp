#include <Processors/QueryPlan/BufferChunksTransform.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

namespace DB
{

ReadInOrderReadAheadTracker::ReadInOrderReadAheadTracker(
    size_t num_inputs, const Block & header, const SortDescription & description_, size_t limit_)
    : description(description_)
    , limit(limit_)
    , inputs(num_inputs)
{
    sort_column_positions.reserve(description.size());
    for (const auto & column_description : description)
        sort_column_positions.push_back(header.getPositionByName(column_description.column_name));
}

void ReadInOrderReadAheadTracker::setFirstRow(size_t input, const Chunk & chunk)
{
    chassert(input < inputs.size());
    auto & state = inputs[input];
    chassert(state.first_row.empty() && chunk.getNumRows() > 0);

    const auto & columns = chunk.getColumns();
    state.first_row.reserve(sort_column_positions.size());
    /// Without `LowCardinality`, so that one row does not keep the whole dictionary alive.
    for (size_t position : sort_column_positions)
        state.first_row.push_back(recursiveRemoveLowCardinality(columns[position]->cut(0, 1)->convertToFullIfWrapped()));

    size_t slot = num_published.fetch_add(1, std::memory_order_relaxed);
    if (slot < published.size())
        published[slot].store(input + 1, std::memory_order_release);
}

void ReadInOrderReadAheadTracker::setProducedRows(size_t input, size_t rows)
{
    inputs[input].produced.store(rows, std::memory_order_relaxed);
}

int ReadInOrderReadAheadTracker::compareFirstRows(size_t lhs, size_t rhs) const
{
    for (size_t i = 0; i < description.size(); ++i)
    {
        if (int res = description[i].direction
                * inputs[lhs].first_row[i]->compareAt(0, 0, *inputs[rhs].first_row[i], description[i].nulls_direction))
            return res;
    }
    return 0;
}

size_t ReadInOrderReadAheadTracker::rowsProducedBefore(size_t input, InputsBefore & inputs_before) const
{
    chassert(!inputs[input].first_row.empty());

    for (; inputs_before.num_compared < published.size(); ++inputs_before.num_compared)
    {
        size_t entry = published[inputs_before.num_compared].load(std::memory_order_acquire);
        if (entry == 0)
            break;

        if (compareFirstRows(entry - 1, input) < 0)
            inputs_before.inputs.push_back(entry - 1);
    }

    size_t rows = 0;
    for (size_t j : inputs_before.inputs)
        rows += inputs[j].produced.load(std::memory_order_relaxed);
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

        input.setNeeded();

        if (!chunks.empty())
        {
            auto chunk = std::move(chunks.front());
            chunks.pop();

            if (isVirtualRow(chunk))
            {
                output.push(std::move(chunk));
                output_has_our_chunk = true;
                wait_for_demand_after_virtual_row = true;
                input.setNotNeeded();
                return Status::PortFull;
            }

            num_buffered_rows -= chunk.getNumRows();
            num_buffered_bytes -= chunk.bytes();

            output.push(std::move(chunk));
            output_has_our_chunk = true;
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

    /// Pausing is safe only while the output port is not ready: the merge taking our chunk
    /// or asking for data reschedules us.
    if (read_ahead_tracker && first_row_published && !output.canPush() && chunks_taken_by_merge <= 1
        && read_ahead_tracker->rowsProducedBefore(input_index, inputs_before) + num_processed_rows >= read_ahead_tracker->getLimit())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (input.hasData() && (num_buffered_rows < max_rows_to_buffer || num_buffered_bytes < max_bytes_to_buffer))
    {
        bool virtual_row = false;
        auto chunk = pullChunk(virtual_row);
        if (virtual_row)
        {
            chunks.push(std::move(chunk));
            input.setNotNeeded();
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

Chunk BufferChunksTransform::pullChunk(bool & virtual_row)
{
    auto chunk = input.pull();
    virtual_row = isVirtualRow(chunk);
    if (!virtual_row)
    {
        num_processed_rows += chunk.getNumRows();
        if (read_ahead_tracker)
        {
            read_ahead_tracker->setProducedRows(input_index, num_processed_rows);
            if (!first_row_published && chunk.getNumRows() > 0)
            {
                read_ahead_tracker->setFirstRow(input_index, chunk);
                first_row_published = true;
            }
        }
    }

    if (limit && num_processed_rows >= limit)
        input.close();

    return chunk;
}

}
