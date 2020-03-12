#include <Processors/LimitTransform.h>


namespace DB
{

LimitTransform::LimitTransform(
    const Block & header_, size_t limit_, size_t offset_, size_t num_streams,
    bool always_read_till_end_, bool with_ties_,
    SortDescription description_)
    : IProcessor({header_}, {header_})
    , limit(limit_), offset(offset_)
    , always_read_till_end(always_read_till_end_)
    , with_ties(with_ties_), description(std::move(description_))
{
    input_ports.reserve(num_streams);
    output_ports.reserve(num_streams);

    for (auto & input : inputs)
        input_ports.emplace_back(&input);

    for (auto & output : outputs)
        output_ports.emplace_back(&output);

    is_port_pair_finished.assign(num_streams, false);

    for (const auto & desc : description)
    {
        if (!desc.column_name.empty())
            sort_column_positions.push_back(header_.getPositionByName(desc.column_name));
        else
            sort_column_positions.push_back(desc.column_number);
    }
}

Chunk LimitTransform::makeChunkWithPreviousRow(const Chunk & chunk, size_t row) const
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


IProcessor::Status LimitTransform::prepare(
        const PortNumbers & updated_input_ports,
        const PortNumbers & updated_output_ports)
{
    bool has_full_port = false;

    auto process_pair = [&](size_t pos)
    {
        auto status = preparePair(*input_ports[pos], *output_ports[pos]);

        switch (status)
        {
            case IProcessor::Status::Finished:
            {
                if (!is_port_pair_finished[pos])
                {
                    is_port_pair_finished[pos] = true;
                    ++num_finished_port_pairs;
                }

                return;
            }
            case IProcessor::Status::PortFull:
            {
                has_full_port = true;
                return;
            }
            case IProcessor::Status::NeedData:
                return;
            default:
                throw Exception(
                        "Unexpected status for LimitTransform::preparePair : " + IProcessor::statusToName(status),
                        ErrorCodes::LOGICAL_ERROR);

        }

        __builtin_unreachable();
    };

    for (auto pos : updated_input_ports)
        process_pair(pos);

    for (auto pos : updated_output_ports)
        process_pair(pos);

    if (num_finished_port_pairs == input_ports.size())
        return Status::Finished;

    if (has_full_port)
        return Status::PortFull;

    return Status::NeedData;
}

LimitTransform::Status LimitTransform::preparePair(InputPort & input, OutputPort & output)
{
    /// Check can output.
    bool output_finished = false;
    if (output.isFinished())
    {
        output_finished = true;
        if (!always_read_till_end)
        {
            input.close();
            return Status::Finished;
        }
    }

    if (!output_finished && !output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Push block if can.
    if (!output_finished && has_block && block_processed)
    {
        output.push(std::move(current_chunk));
        has_block = false;
        block_processed = false;
    }

    /// Check if we are done with pushing.
    bool is_limit_reached = (rows_read >= offset + limit) && !previous_row_chunk;
    if (is_limit_reached)
    {
        if (!always_read_till_end)
        {
            output.finish();
            input.close();
            return Status::Finished;
        }
    }

    /// Check can input.

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_block = true;

    auto rows = current_chunk.getNumRows();
    rows_before_limit_at_least += rows;

    /// Skip block (for 'always_read_till_end' case).
    if (is_limit_reached)
    {
        current_chunk.clear();
        has_block = false;

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    /// Process block.

    rows_read += rows;

    if (rows_read <= offset)
    {
        current_chunk.clear();
        has_block = false;

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    if (output.hasData())
        return Status::PortFull;

    if (rows_read >= offset + rows && rows_read <= offset + limit)
    {
        /// Return the whole chunk.

        /// Save the last row of current chunk to check if next block begins with the same row (for WITH TIES).
        if (with_ties && rows_read == offset + limit)
            previous_row_chunk = makeChunkWithPreviousRow(current_chunk, current_chunk.getNumRows() - 1);
    }
    else
        splitChunk();

    bool may_need_more_data_for_ties = previous_row_chunk || rows_read - rows <= offset + limit;
    /// No more data is needed.
    if (!always_read_till_end && (rows_read >= offset + limit) && !may_need_more_data_for_ties)
        input.close();

    output.push(std::move(current_chunk));
    has_block = false;

    return Status::PortFull;
}


void LimitTransform::splitChunk()
{
    auto current_chunk_sort_columns = extractSortColumns(current_chunk.getColumns());
    size_t num_rows = current_chunk.getNumRows();
    size_t num_columns = current_chunk.getNumColumns();

    if (previous_row_chunk && rows_read >= offset + limit)
    {
        /// Scan until the first row, which is not equal to previous_row_chunk (for WITH TIES)
        size_t current_row_num = 0;
        for (; current_row_num < num_rows; ++current_row_num)
        {
            if (!sortColumnsEqualAt(current_chunk_sort_columns, current_row_num))
                break;
        }

        auto columns = current_chunk.detachColumns();

        if (current_row_num < num_rows)
        {
            previous_row_chunk = {};
            for (size_t i = 0; i < num_columns; ++i)
                columns[i] = columns[i]->cut(0, current_row_num);
        }

        current_chunk.setColumns(std::move(columns), current_row_num);
        block_processed = true;
        return;
    }

    /// return a piece of the block
    size_t start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(rows_read) + static_cast<Int64>(num_rows));

    size_t length = std::min(
        static_cast<Int64>(limit), std::min(
        static_cast<Int64>(rows_read) - static_cast<Int64>(offset),
        static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(rows_read) + static_cast<Int64>(num_rows)));

    /// check if other rows in current block equals to last one in limit
    if (with_ties && length)
    {
        size_t current_row_num = start + length;
        previous_row_chunk = makeChunkWithPreviousRow(current_chunk, current_row_num - 1);

        for (; current_row_num < num_rows; ++current_row_num)
        {
            if (!sortColumnsEqualAt(current_chunk_sort_columns, current_row_num))
            {
                previous_row_chunk = {};
                break;
            }
        }

        length = current_row_num - start;
    }

    if (length == num_rows)
    {
        block_processed = true;
        return;
    }

    auto columns = current_chunk.detachColumns();

    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    current_chunk.setColumns(std::move(columns), length);

    block_processed = true;
}

ColumnRawPtrs LimitTransform::extractSortColumns(const Columns & columns) const
{
    ColumnRawPtrs res;
    res.reserve(description.size());
    for (size_t pos : sort_column_positions)
        res.push_back(columns[pos].get());

    return res;
}

bool LimitTransform::sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, size_t current_chunk_row_num) const
{
    assert(current_chunk_sort_columns.size() == previous_row_chunk.getNumColumns());
    size_t size = current_chunk_sort_columns.size();
    const auto & previous_row_sort_columns = previous_row_chunk.getColumns();
    for (size_t i = 0; i < size; ++i)
        if (0 != current_chunk_sort_columns[i]->compareAt(current_chunk_row_num, 0, *previous_row_sort_columns[i], 1))
            return false;
    return true;
}

}

