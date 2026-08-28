#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/FractionalLimitTransform.h>

#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void FractionalLimitTransform::finalizeLimits()
{
    /// Fractions depend on the final total number of rows, so we can only compute the integral
    /// limit/offset once all input is read.
    if (limits_are_final)
        return;

    limit_rows = static_cast<UInt64>(std::ceil(static_cast<double>(total_input_rows) * limit_fraction));
    offset_rows += static_cast<UInt64>(std::ceil(static_cast<double>(total_input_rows) * offset_fraction));

    if (with_ties && rows_processed < limit_rows + offset_rows)
        ties_last_row = {};

    limits_are_final = true;
}

FractionalLimitTransform::FractionalLimitTransform(
    SharedHeader header_,
    Float64 limit_fraction_,
    Float64 offset_fraction_,
    UInt64 offset_,
    size_t num_streams,
    bool with_ties_,
    SortDescription limit_with_ties_sort_description_)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , limit_fraction(limit_fraction_)
    , offset_fraction(offset_fraction_)
    , offset_rows(offset_)
    , with_ties(with_ties_)
    , limit_with_ties_sort_description(std::move(limit_with_ties_sort_description_))
{
    if (limit_fraction <= 0.0 || limit_fraction >= 1.0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fractional LIMIT values must be in the range (0, 1)");

    if (offset_fraction < 0.0 || offset_fraction >= 1.0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Fractional OFFSET values must be in the range (0, 1)");

    if (num_streams != 1 && with_ties)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot use FractionalLimitTransform with multiple ports and ties");

    ports_data.resize(num_streams);

    size_t stream_idx = 0;
    for (auto & input : inputs)
    {
        ports_data[stream_idx].input_port = &input;
        ++stream_idx;
    }

    stream_idx = 0;
    for (auto & output : outputs)
    {
        ports_data[stream_idx].output_port = &output;
        ++stream_idx;
    }

    for (const auto & desc : limit_with_ties_sort_description)
        sort_key_positions.push_back(header_->getPositionByName(desc.column_name));
}

Chunk FractionalLimitTransform::makeChunkWithPreviousRow(const Chunk & chunk, UInt64 row) const
{
    assert(row < chunk.getNumRows());
    ColumnRawPtrs current_columns = extractSortColumns(chunk.getColumns());
    MutableColumns last_row_sort_columns;
    for (size_t i = 0; i < current_columns.size(); ++i)
    {
        last_row_sort_columns.emplace_back(current_columns[i]->cloneEmpty());
        last_row_sort_columns[i]->insertFrom(*current_columns[i], row);
    }
    return Chunk(std::move(last_row_sort_columns), 1);
}


FractionalLimitTransform::Status FractionalLimitTransform::prepare()
{
    if (allOutputsFinished())
    {
        /// Nobody needs data: stop sources.
        for (auto & port : ports_data)
            port.input_port->close();
        return Status::Finished;
    }

    /// Check can we still pull data from input?
    if (num_finished_input_ports != ports_data.size())
    {
        auto process_port = [&](size_t port_idx)
        {
            auto status = pullData(ports_data[port_idx]);

            switch (status)
            {
                case IProcessor::Status::Finished:
                {
                    if (!ports_data[port_idx].is_input_finished)
                    {
                        ports_data[port_idx].is_input_finished = true;
                        ++num_finished_input_ports;
                    }
                    return;
                }
                case IProcessor::Status::NeedData:
                    return;
                default:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected status for FractionalLimitTransform::pullData : {}",
                        IProcessor::statusToName(status));
            }
        };

        for (size_t port_idx = 0; port_idx < ports_data.size(); ++port_idx)
            process_port(port_idx);

        /// Without fractional offset we can push chunks early before reading all inputs.
        if (offset_fraction == 0.0)
        {
            bool pushed_any = false;
            while (!cached_chunks.empty())
            {
                auto * output = getAvailableOutputPort();
                if (!output)
                {
                    if (num_finished_input_ports == ports_data.size())
                        finalizeLimits();

                    return Status::PortFull;
                }

                auto & front_chunk = cached_chunks.front();
                UInt64 chunk_rows = front_chunk.getNumRows();

                /// When integral OFFSET ends inside the first cached chunk, we must skip that prefix.
                UInt64 offset_rows_remaining = 0;
                if (rows_processed < offset_rows)
                    offset_rows_remaining = offset_rows - rows_processed;

                /// If we push this whole chunk now, would it exceed the current fractional LIMIT
                /// computed from the number of rows already read?
                const UInt64 limit_rows_for_current_input = static_cast<UInt64>(
                    std::ceil(static_cast<double>(total_input_rows) * limit_fraction));
                if (limit_rows_for_current_input + offset_rows_remaining < chunk_rows + early_pushed_rows)
                    break;

                /// If we still have an integral offset that didn't cause the chunk
                /// to be dropped entirely above then its offset is in part of the chunk => split it
                /// Notice that it only happens once
                if (offset_rows_remaining)
                {
                    /// offset_rows_remaining is guaranteed to be < chunk_rows here: cached_chunks.front() is the
                    /// first chunk that crosses the integral OFFSET boundary. Otherwise the whole chunk would still
                    /// be within OFFSET and would have been dropped in pullData().
                    if (offset_rows_remaining >= chunk_rows)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Remaining offset ({}) must be less than chunk rows ({})",
                            offset_rows_remaining,
                            chunk_rows);

                    const UInt64 num_columns = front_chunk.getNumColumns();
                    auto columns = front_chunk.detachColumns();

                    const UInt64 rows_after_offset = chunk_rows - offset_rows_remaining;
                    for (UInt64 i = 0; i < num_columns; ++i)
                        columns[i] = columns[i]->cut(offset_rows_remaining, rows_after_offset);
                    front_chunk.setColumns(std::move(columns), rows_after_offset);

                    chunk_rows = rows_after_offset;
                    rows_processed += offset_rows_remaining;
                }

                rows_processed += chunk_rows;
                early_pushed_rows += chunk_rows;
                if (with_ties && rows_processed == limit_rows_for_current_input + offset_rows)
                    ties_last_row = makeChunkWithPreviousRow(front_chunk, chunk_rows - 1);

                output->push(std::move(front_chunk));
                cached_chunks.pop_front();
                pushed_any = true;
            }

            if (pushed_any)
            {
                if (num_finished_input_ports == ports_data.size())
                    finalizeLimits();
                return Status::PortFull;
            }
        }

        if (num_finished_input_ports != ports_data.size())
            /// Some input ports still available => we can read more data
            return Status::NeedData;

        finalizeLimits();
    }
    else if (!limits_are_final)
    {
        finalizeLimits();
    }

    /// If we reached here all input ports are finished.
    /// we start pushing cached chunks to output ports.
    auto status = pushData();
    if (status != Status::Finished)
        return status;

    for (auto & port : ports_data)
    {
        port.input_port->close();
        port.output_port->finish();
    }

    return Status::Finished;
}

bool FractionalLimitTransform::allOutputsFinished() const
{
    for (const auto & data : ports_data)
        if (!data.output_port->isFinished())
            return false;
    return true;
}

OutputPort * FractionalLimitTransform::getAvailableOutputPort()
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

FractionalLimitTransform::Status FractionalLimitTransform::pullData(PortsData & data)
{
    auto & input = *data.input_port;

    /// Check can input?
    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    data.current_chunk = input.pull();

    const UInt64 chunk_rows = data.current_chunk.getNumRows();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(chunk_rows);

    /// Process block.

    total_input_rows += chunk_rows;

    /// Ignore chunk if it should be offsetted
    if (total_input_rows <= offset_rows)
    {
        /// As if it was put in cache then evicted due to offset.
        rows_processed += chunk_rows;

        data.current_chunk.clear();

        if (input.isFinished())
            return Status::Finished;

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    cached_chunks.push_back(std::move(data.current_chunk));

    /// This optimizes if offset_fraction is set but the above block optimizes if integral offset is set and both can't be non-zero.
    ///
    /// Detect blocks that will 100% get removed by the fractional offset and remove them as early as possible.
    /// Example: if we have 10 blocks with same num of rows and offset 0.1 we can freely drop the first block even before reading all data.
    const UInt64 fractional_offset_rows = static_cast<UInt64>(std::ceil(static_cast<double>(total_input_rows) * offset_fraction));
    while (!cached_chunks.empty() && fractional_offset_rows >= cached_chunks.front().getNumRows() + rows_processed)
    {
        rows_processed += cached_chunks.front().getNumRows();
        cached_chunks.pop_front();
    }

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    return Status::NeedData;
}

FractionalLimitTransform::Status FractionalLimitTransform::pushData()
{
    /// Drain the cache while we have an output that can accept data. Return PortFull only if
    /// all outputs are currently blocked; output finishing is handled by prepare().
    while (!cached_chunks.empty())
    {
        /// Check if we reached limit.
        const bool is_limit_reached = rows_processed >= offset_rows + limit_rows && !ties_last_row;
        if (is_limit_reached)
            return Status::Finished;

        auto * output = getAvailableOutputPort();
        if (!output)
            return Status::PortFull;

        /// The early removal of blocks by offset and fractional_offset at pullData() should have detected
        /// all chunks that will be dropped entirely, but we may still need to offset inside the first block
        /// and drop a portion of it.

        auto & chunk = cached_chunks.front();
        const UInt64 chunk_rows = chunk.getNumRows();
        rows_processed += chunk_rows;

        if (chunk_rows <= std::numeric_limits<UInt64>::max() - offset_rows && rows_processed >= offset_rows + chunk_rows
            && rows_processed <= offset_rows + limit_rows)
        {
            /// Return the whole chunk.
            /// Save the last row of current chunk to check if next block begins with the same row (for WITH TIES).
            if (with_ties && rows_processed == offset_rows + limit_rows)
                ties_last_row = makeChunkWithPreviousRow(chunk, chunk_rows - 1);
        }
        else
            /// This function may be heavy to execute. But it happens no more than twice.
            splitChunk(chunk);

        output->push(std::move(chunk));
        cached_chunks.pop_front();
    }

    return Status::Finished;
}


void FractionalLimitTransform::splitChunk(Chunk & current_chunk)
{
    auto current_chunk_sort_columns = extractSortColumns(current_chunk.getColumns());
    const UInt64 chunk_rows = current_chunk.getNumRows();
    const UInt64 num_columns = current_chunk.getNumColumns();

    if (ties_last_row && rows_processed >= offset_rows + limit_rows)
    {
        /// Scan until the first row, which is not equal to ties_last_row (for WITH TIES)
        UInt64 current_row_num = 0;
        for (; current_row_num < chunk_rows; ++current_row_num)
        {
            if (!sortColumnsEqualAt(current_chunk_sort_columns, current_row_num))
                break;
        }

        auto columns = current_chunk.detachColumns();

        if (current_row_num < chunk_rows)
        {
            ties_last_row = {};
            for (UInt64 i = 0; i < num_columns; ++i)
                columns[i] = columns[i]->cut(0, current_row_num);
        }

        current_chunk.setColumns(std::move(columns), current_row_num);
        return;
    }

    /// return a piece of the block
    UInt64 cut_start = 0;

    /// ------------[....(...).]
    /// <----------------------> rows_processed
    ///             <----------> chunk_rows
    /// <---------------> offset_rows
    ///             <---> cut_start

    assert(offset_rows < rows_processed);

    if (offset_rows + chunk_rows > rows_processed)
        cut_start = offset_rows + chunk_rows - rows_processed;

    /// ------------[....(...).]
    /// <----------------------> rows_processed
    ///             <----------> chunk_rows
    /// <---------------> offset_rows
    ///                  <---> limit
    ///                  <---> cut_length
    ///             <---> cut_start

    /// Or:

    /// -----------------(------[....)....]
    /// <---------------------------------> rows_processed
    ///                         <---------> chunk_rows
    /// <---------------> offset_rows
    ///                  <-----------> limit
    ///                         <----> cut_length
    ///                         0 = cut_start

    UInt64 cut_length = chunk_rows - cut_start;

    if (offset_rows + limit_rows < rows_processed)
    {
        if (offset_rows + limit_rows < rows_processed - chunk_rows)
            cut_length = 0;
        else
            cut_length = offset_rows + limit_rows - (rows_processed - chunk_rows) - cut_start;
    }

    /// Check if other rows in current block equals to last one in limit
    /// when rows_processed >= offset_rows + limit_rows.
    if (with_ties && offset_rows + limit_rows <= rows_processed && cut_length)
    {
        UInt64 current_row_num = cut_start + cut_length;
        ties_last_row = makeChunkWithPreviousRow(current_chunk, current_row_num - 1);

        for (; current_row_num < chunk_rows; ++current_row_num)
        {
            if (!sortColumnsEqualAt(current_chunk_sort_columns, current_row_num))
            {
                ties_last_row = {};
                break;
            }
        }

        cut_length = current_row_num - cut_start;
    }

    if (cut_length == chunk_rows)
        return;

    auto columns = current_chunk.detachColumns();

    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(cut_start, cut_length);

    current_chunk.setColumns(std::move(columns), cut_length);
}

ColumnRawPtrs FractionalLimitTransform::extractSortColumns(const Columns & columns) const
{
    ColumnRawPtrs res;
    res.reserve(limit_with_ties_sort_description.size());
    for (size_t pos : sort_key_positions)
        res.push_back(columns[pos].get());

    return res;
}

bool FractionalLimitTransform::sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, UInt64 current_chunk_row_num) const
{
    assert(current_chunk_sort_columns.size() == ties_last_row.getNumColumns());
    const size_t num_sort_columns = current_chunk_sort_columns.size();
    const auto & ties_last_row_sort_columns = ties_last_row.getColumns();
    for (size_t i = 0; i < num_sort_columns; ++i)
        if (0 != current_chunk_sort_columns[i]->compareAt(current_chunk_row_num, 0, *ties_last_row_sort_columns[i], 1))
            return false;
    return true;
}

}
