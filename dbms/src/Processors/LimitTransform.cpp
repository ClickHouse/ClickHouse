#include <Processors/LimitTransform.h>


namespace DB
{

LimitTransform::LimitTransform(
    const Block & header, size_t limit, size_t offset,
    bool always_read_till_end, bool do_count_rows_before_limit)
    : IProcessor({header}, {header})
    , input(inputs.front()), output(outputs.front())
    , limit(limit), offset(offset)
    , always_read_till_end(always_read_till_end)
    , do_count_rows_before_limit(do_count_rows_before_limit)
{
}


LimitTransform::Status LimitTransform::prepare()
{
    /// Check if we are done with pushing.
    bool pushing_is_finished = rows_read >= offset + limit;

    /// Check can output.

    if (output.isFinished())
    {
        pushing_is_finished = true;
        if (!always_read_till_end)
        {
            input.close();
            return Status::Finished;
        }
    }

    if (!pushing_is_finished && !output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Push block if can.
    if (!pushing_is_finished && has_block && block_processed)
    {
        output.push(std::move(current_chunk));
        has_block = false;
        block_processed = false;
    }

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
    if (do_count_rows_before_limit)
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

