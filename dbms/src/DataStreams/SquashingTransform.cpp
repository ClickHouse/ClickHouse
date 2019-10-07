#include <DataStreams/SquashingTransform.h>


namespace DB
{

SquashingTransform::SquashingTransform(size_t min_block_size_rows, size_t min_block_size_bytes)
    : min_block_size_rows(min_block_size_rows), min_block_size_bytes(min_block_size_bytes)
{
}


SquashingTransform::Result SquashingTransform::add(MutableColumns && columns)
{
    /// End of input stream.
    if (columns.empty())
        return Result(std::move(accumulated_columns));

    /// Just read block is alredy enough.
    if (isEnoughSize(columns))
    {
        /// If no accumulated data, return just read block.
        if (accumulated_columns.empty())
            return Result(std::move(columns));

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        columns.swap(accumulated_columns);
        return Result(std::move(columns));
    }

    /// Accumulated block is already enough.
    if (!accumulated_columns.empty() && isEnoughSize(accumulated_columns))
    {
        /// Return accumulated data and place new block to accumulated data.
        columns.swap(accumulated_columns);
        return Result(std::move(columns));
    }

    append(std::move(columns));

    if (isEnoughSize(accumulated_columns))
    {
        MutableColumns res;
        res.swap(accumulated_columns);
        return Result(std::move(res));
    }

    /// Squashed block is not ready.
    return false;
}


void SquashingTransform::append(MutableColumns && columns)
{
    if (accumulated_columns.empty())
    {
        accumulated_columns = std::move(columns);
        return;
    }

    for (size_t i = 0, size = columns.size(); i < size; ++i)
        accumulated_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
}


bool SquashingTransform::isEnoughSize(const MutableColumns & columns)
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
