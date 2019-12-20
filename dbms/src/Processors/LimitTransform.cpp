#include <Processors/LimitTransform.h>


namespace DB
{

LimitTransform::LimitTransform(
    const Block & header_, size_t limit_, size_t offset_,
    bool always_read_till_end_, bool with_ties_,
    const SortDescription & description_)
    : IProcessor({header_}, {header_})
    , input(inputs.front()), output(outputs.front())
    , limit(limit_), offset(offset_)
    , always_read_till_end(always_read_till_end_)
    , with_ties(with_ties_), description(description_)
{
    for (const auto & desc : description)
    {
        if (!desc.column_name.empty())
            sort_column_positions.push_back(header_.getPositionByName(desc.column_name));
        else
            sort_column_positions.push_back(desc.column_number);
    }
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

    /// Check if we are done with pushing.
    bool pushing_is_finished = (rows_read >= offset + limit) && ties_row_ref.empty();
    if (pushing_is_finished)
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

    current_chunk = input.pull();
    has_block = true;

    auto rows = current_chunk.getNumRows();
    rows_before_limit_at_least += rows;

    /// Skip block (for 'always_read_till_end' case).
    if (pushing_is_finished)
    {
        current_chunk.clear();
        has_block = false;

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
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
        return Status::NeedData;
    }

    /// Return the whole block.
    if (rows_read >= offset + rows && rows_read <= offset + limit)
    {
        if (output.hasData())
            return Status::PortFull;

        if (with_ties && rows_read == offset + limit)
        {
            SharedChunkPtr shared_chunk = new detail::SharedChunk(current_chunk.clone());
            shared_chunk->sort_columns = extractSortColumns(shared_chunk->getColumns());
            ties_row_ref.set(shared_chunk, &shared_chunk->sort_columns, shared_chunk->getNumRows() - 1);
        }

        output.push(std::move(current_chunk));
        has_block = false;

        return Status::PortFull;
    }

    /// No more data is needed.
    if (!always_read_till_end && rows_read >= offset + limit)
        input.close();

    return Status::Ready;
}


void LimitTransform::work()
{
    SharedChunkPtr shared_chunk = new detail::SharedChunk(std::move(current_chunk));
    shared_chunk->sort_columns = extractSortColumns(shared_chunk->getColumns());

    size_t num_rows = shared_chunk->getNumRows();
    size_t num_columns = shared_chunk->getNumColumns();

    if (!ties_row_ref.empty() && rows_read >= offset + limit)
    {
        UInt64 len;
        for (len = 0; len < num_rows; ++len)
        {
            SharedChunkRowRef current_row;
            current_row.set(shared_chunk, &shared_chunk->sort_columns, len);

            if (current_row != ties_row_ref)
            {
                ties_row_ref.reset();
                break;
            }
        }

        auto columns = shared_chunk->detachColumns();

        if (len < num_rows)
        {
            for (size_t i = 0; i < num_columns; ++i)
                columns[i] = columns[i]->cut(0, len);
        }

        current_chunk.setColumns(std::move(columns), len);
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
    if (with_ties)
    {
        ties_row_ref.set(shared_chunk, &shared_chunk->sort_columns, start + length - 1);
        SharedChunkRowRef current_row;

        for (size_t i = ties_row_ref.row_num + 1; i < num_rows; ++i)
        {
            current_row.set(shared_chunk, &shared_chunk->sort_columns, i);
            if (current_row == ties_row_ref)
                ++length;
            else
            {
                ties_row_ref.reset();
                break;
            }
        }
    }

    if (length == num_rows)
    {
        current_chunk = std::move(*shared_chunk);
        block_processed = true;
        return;
    }

    auto columns = shared_chunk->detachColumns();

    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    current_chunk.setColumns(std::move(columns), length);

    block_processed = true;
}

ColumnRawPtrs LimitTransform::extractSortColumns(const Columns & columns)
{
    ColumnRawPtrs res;
    res.reserve(description.size());
    for (size_t pos : sort_column_positions)
        res.push_back(columns[pos].get());

    return res;
}

}

