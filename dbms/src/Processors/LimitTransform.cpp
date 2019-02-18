#include <Processors/LimitTransform.h>


namespace DB
{

LimitTransform::LimitTransform(Block header, size_t limit, size_t offset, bool always_read_till_end)
    : IProcessor({std::move(header)}, {std::move(header)}),
    input(inputs.front()), output(outputs.front()),
    limit(limit), offset(offset), always_read_till_end(always_read_till_end)
{
}


LimitTransform::Status LimitTransform::prepare()
{
    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Push block if can.
    if (block_processed)
    {
        output.push(std::move(current_chunk));
        has_block = false;
        block_processed = false;
    }

    /// Check if we are done with pushing.
    bool pushing_is_finished = rows_read >= offset + limit;
    if (pushing_is_finished)
    {
        output.finish();
        if (!always_read_till_end)
        {
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

    /// Skip block (for 'always_read_till_end' case).
    if (pushing_is_finished)
    {
        current_chunk.clear();
        has_block = false;

        /// Now, we pulled from input, and it must be empty.
        return Status::NeedData;
    }

    /// Process block.

    size_t rows = current_chunk.getNumRows();
    rows_read += rows;

    if (rows_read <= offset)
    {
        current_chunk.clear();
        has_block = false;

        /// Now, we pulled from input, and it must be empty.
        return Status::NeedData;
    }

    /// Return the whole block.
    if (rows_read >= offset + rows && rows_read <= offset + limit)
    {
        if (output.hasData())
            return Status::PortFull;

        output.push(std::move(current_chunk));
        has_block = false;

        return Status::NeedData;
    }

    return Status::Ready;
}


void LimitTransform::work()
{
    size_t num_rows = current_chunk.getNumRows();
    size_t num_columns = current_chunk.getNumColumns();

    /// return a piece of the block
    size_t start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(rows_read) + static_cast<Int64>(num_rows));

    size_t length = std::min(
        static_cast<Int64>(limit), std::min(
        static_cast<Int64>(rows_read) - static_cast<Int64>(offset),
        static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(rows_read) + static_cast<Int64>(num_rows)));

    auto columns = current_chunk.detachColumns();

    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    current_chunk.setColumns(std::move(columns), length);

    block_processed = true;
}

}

