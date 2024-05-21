#include <Interpreters/SquashingTransform.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

SquashingTransform::SquashingTransform(size_t min_block_size_rows_, size_t min_block_size_bytes_)
    : min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
{
}

SquashingTransform::SquashResult SquashingTransform::add(Block && input_block)
{
    /// End of input stream.
    if (!input_block)
    {
        Block to_return;
        std::swap(to_return, accumulated_block);
        return SquashResult{std::move(to_return), false};
    }

    /// Just read block is already enough.
    if (isEnoughSize(input_block))
    {
        /// If no accumulated data, return just read block.
        if (!accumulated_block)
        {
            return SquashResult{std::move(input_block), false};
        }

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        Block to_return = std::move(input_block);
        std::swap(to_return, accumulated_block);
        return SquashResult{std::move(to_return), true};
    }

    /// Accumulated block is already enough.
    if (isEnoughSize(accumulated_block))
    {
        /// Return accumulated data and place new block to accumulated data.
        Block to_return = std::move(input_block);
        std::swap(to_return, accumulated_block);
        return SquashResult{std::move(to_return), true};
    }

    append(std::move(input_block));
    if (isEnoughSize(accumulated_block))
    {
        Block to_return;
        std::swap(to_return, accumulated_block);
        return SquashResult{std::move(to_return), false};
    }

    /// Squashed block is not ready, input block consumed
    return SquashResult{{}, true};
}


void SquashingTransform::append(Block && input_block)
{
    if (!accumulated_block)
    {
        accumulated_block = std::move(input_block);
        return;
    }

    LOG_DEBUG(getLogger("SquashingTransform"),
              "input_block rows {}, size {}, columns {}, accumulated_block rows {}, size {}, columns {}, ",
              input_block.rows(), input_block.bytes(), input_block.columns(),
              accumulated_block.rows(), accumulated_block.bytes(), accumulated_block.columns());

    assert(blocksHaveEqualStructure(input_block, accumulated_block));

    try
    {
        for (size_t i = 0, size = accumulated_block.columns(); i < size; ++i)
        {
            const auto source_column = input_block.getByPosition(i).column;

            const auto acc_column = accumulated_block.getByPosition(i).column;

            LOG_DEBUG(getLogger("SquashingTransform"),
              "column {} {}, acc rows {}, size {}, allocated {}, input rows {} size {} allocated {}",
                i, source_column->getName(),
                acc_column->size(), acc_column->byteSize(), acc_column->allocatedBytes(),
                source_column->size(), source_column->byteSize(), source_column->allocatedBytes());


            auto mutable_column = IColumn::mutate(std::move(accumulated_block.getByPosition(i).column));
            mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
            accumulated_block.getByPosition(i).column = std::move(mutable_column);
        }
    }
    catch (...)
    {
        /// add() may be called again even after a previous add() threw an exception.
        /// Keep accumulated_block in a valid state.
        /// Seems ok to discard accumulated data because we're throwing an exception, which the caller will
        /// hopefully interpret to mean "this block and all *previous* blocks are potentially lost".
        accumulated_block.clear();
        throw;
    }
}


bool SquashingTransform::isEnoughSize(const Block & block)
{
    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & [column, type, name] : block)
    {
        if (!column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid column in block.");

        if (!rows)
            rows = column->size();
        else if (rows != column->size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Sizes of columns doesn't match");

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
