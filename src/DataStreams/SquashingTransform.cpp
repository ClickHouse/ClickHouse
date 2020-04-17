#include <DataStreams/SquashingTransform.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

SquashingTransform::SquashingTransform(size_t min_block_size_rows_, size_t min_block_size_bytes_, bool reserve_memory_)
    : min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
    , reserve_memory(reserve_memory_)
{
}


Columns SquashingTransform::add(const Block & block)
{
    /// End of input stream.
    if (!block)
    {
        Columns to_return;
        std::swap(to_return, accumulated_columns);
        return to_return;
    }

    auto block_columns = block.getColumns();
    /// Just read block is already enough.
    if (isEnoughSize(block_columns))
    {
        /// If no accumulated data, return just read block.
        if (accumulated_columns.empty())
        {
            return block_columns;
        }

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        block_columns.swap(accumulated_columns);
        return block_columns;
    }

    /// Accumulated block is already enough.
    if (isEnoughSize(accumulated_columns))
    {
        /// Return accumulated data and place new block to accumulated data.
        std::swap(block_columns, accumulated_columns);
        return block_columns;
    }

    append(std::move(block_columns));

    if (isEnoughSize(accumulated_columns))
    {
        Columns to_return;
        std::swap(to_return, accumulated_columns);
        return to_return;
    }

    /// Squashed block is not ready.
    return Columns();
}


void SquashingTransform::append(Columns && block_columns)
{
    if (accumulated_columns.empty())
    {
        std::swap(accumulated_columns, block_columns);
        return;
    }

    assert(block_columns.size() == accumulated_columns.size());

    for (size_t i = 0, size = block_columns.size(); i < size; ++i)
    {
        auto mutable_column = std::move(*accumulated_columns[i]).mutate();

        if (reserve_memory)
        {
            mutable_column->reserve(min_block_size_bytes);
        }
        mutable_column->insertRangeFrom(*block_columns[i], 0,
                                        block_columns[i]->size());

        accumulated_columns[i] = std::move(mutable_column);
    }
}


bool SquashingTransform::isEnoughSize(const Columns & columns)
{
    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & column : columns)
    {
        if (!rows)
            rows = column->size();
        else if (rows != column->size())
            throw Exception("Sizes of columns doesn't match", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        bytes += column->byteSize();
    }

    return isEnoughSize(rows, bytes);
}


bool SquashingTransform::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}

}
