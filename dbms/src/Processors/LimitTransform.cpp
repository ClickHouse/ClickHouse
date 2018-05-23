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
    if (current_block)
    {
        if (output.hasData())
            return Status::PortFull;

        output.push(std::move(current_block));
    }

    if (rows_read >= offset + limit)
    {
        output.setFinished();
        if (!always_read_till_end)
        {
            input.setNotNeeded();
            return Status::Finished;
        }
    }

    if (!output.isNeeded())
        return Status::Unneeded;

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    current_block = input.pull();

    /// Skip block (for 'always_read_till_end' case)
    if (rows_read >= offset + limit)
    {
        current_block.clear();
        return Status::NeedData;
    }

    size_t rows = current_block.rows();
    rows_read += rows;

    if (rows_read <= offset)
    {
        current_block.clear();
        return Status::NeedData;
    }

    /// return the whole block
    if (rows_read >= offset + rows && rows_read <= offset + limit)
    {
        if (output.hasData())
            return Status::PortFull;

        output.push(std::move(current_block));
        return Status::NeedData;
    }

    return Status::Ready;
}


void LimitTransform::work()
{
    size_t rows = current_block.rows();
    size_t columns = current_block.columns();

    /// return a piece of the block
    size_t start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(rows_read) + static_cast<Int64>(rows));

    size_t length = std::min(
        static_cast<Int64>(limit), std::min(
        static_cast<Int64>(rows_read) - static_cast<Int64>(offset),
        static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(rows_read) + static_cast<Int64>(rows)));

    for (size_t i = 0; i < columns; ++i)
        current_block.getByPosition(i).column = current_block.getByPosition(i).column->cut(start, length);
}

}

