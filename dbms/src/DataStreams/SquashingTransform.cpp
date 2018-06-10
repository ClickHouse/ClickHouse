#include <DataStreams/SquashingTransform.h>


namespace DB
{

SquashingTransform::SquashingTransform(size_t min_block_size_rows, size_t min_block_size_bytes)
    : min_block_size_rows(min_block_size_rows), min_block_size_bytes(min_block_size_bytes)
{
}


SquashingTransform::Result SquashingTransform::add(Block && block)
{
    if (!block)
        return Result(std::move(accumulated_block));

    /// Just read block is alredy enough.
    if (isEnoughSize(block.rows(), block.bytes()))
    {
        /// If no accumulated data, return just read block.
        if (!accumulated_block)
            return Result(std::move(block));

        /// Return accumulated data (may be it has small size) and place new block to accumulated data.
        accumulated_block.swap(block);
        return Result(std::move(block));
    }

    /// Accumulated block is already enough.
    if (accumulated_block && isEnoughSize(accumulated_block.rows(), accumulated_block.bytes()))
    {
        /// Return accumulated data and place new block to accumulated data.
        accumulated_block.swap(block);
        return Result(std::move(block));
    }

    append(std::move(block));

    if (isEnoughSize(accumulated_block.rows(), accumulated_block.bytes()))
    {
        Block res;
        res.swap(accumulated_block);
        return Result(std::move(res));
    }

    /// Squashed block is not ready.
    return false;
}


void SquashingTransform::append(Block && block)
{
    if (!accumulated_block)
    {
        accumulated_block = std::move(block);
        return;
    }

    size_t columns = block.columns();
    size_t rows = block.rows();

    for (size_t i = 0; i < columns; ++i)
    {
        MutableColumnPtr mutable_column = (*std::move(accumulated_block.getByPosition(i).column)).mutate();
        mutable_column->insertRangeFrom(*block.getByPosition(i).column, 0, rows);
        accumulated_block.getByPosition(i).column = std::move(mutable_column);
    }
}


bool SquashingTransform::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}

}
