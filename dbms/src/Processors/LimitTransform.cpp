#include <Processors/LimitTransform.h>


namespace DB
{

LimitTransform::LimitTransform(
    const Block & header_, size_t limit_, size_t offset_, LimitStatePtr limit_state_,
    bool always_read_till_end_, bool with_ties_,
    SortDescription description_)
    : IProcessor({header_}, {header_})
    , input(inputs.front()), output(outputs.front())
    , limit(limit_), offset(offset_), limit_state(std::move(limit_state_))
    , always_read_till_end(always_read_till_end_)
    , with_ties(with_ties_), description(std::move(description_))
{
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


LimitTransform::Status LimitTransform::prepare()
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

    /// Update the number of read rows.
    if (limit_state)
        rows_read = limit_state->total_read_rows.load(std::memory_order_acquire);

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

    if (limit_state)
        /// Note: maybe memory_order_relaxed is enough. It is needed to be proven.
        rows_read = limit_state->total_read_rows.fetch_add(rows, std::memory_order_acq_rel) + rows;
    else
        rows_read += rows;

    /// rows_read could be updated after previous load. Recalculate flag again.
    is_limit_reached = (rows_read >= offset + limit + rows) && !previous_row_chunk;

    /// Skip block (for 'always_read_till_end' case).
    if (is_limit_reached)
    {
        current_chunk.clear();
        has_block = false;

        if (input.isFinished() || !always_read_till_end)
        {
            output.finish();
            input.close();
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    /// Process block.
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

    /// Return the whole block.
    if (rows_read >= offset + rows && rows_read <= offset + limit)
    {
        if (output.hasData())
            return Status::PortFull;

        /// Save the last row of current block to check if next block begins with the same row (for WITH TIES).
        if (with_ties && rows_read == offset + limit)
            previous_row_chunk = makeChunkWithPreviousRow(current_chunk, current_chunk.getNumRows() - 1);

        output.push(std::move(current_chunk));
        has_block = false;

        return Status::PortFull;
    }

    bool may_need_more_data_for_ties = previous_row_chunk || rows_read - rows <= offset + limit;
    /// No more data is needed.
    if (!always_read_till_end && (rows_read >= offset + limit) && !may_need_more_data_for_ties)
        input.close();

    return Status::Ready;
}


void LimitTransform::work()
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

