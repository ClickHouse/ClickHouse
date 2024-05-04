#include <Interpreters/SquashingTransform.h>


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

Block SquashingTransform::add(Block && input_block)
{
    return addImpl<Block &&>(std::move(input_block));
}

Block SquashingTransform::add(const Block & input_block)
{
    return addImpl<const Block &>(input_block);
}

/*
 * To minimize copying, accept two types of argument: const reference for output
 * stream, and rvalue reference for input stream, and decide whether to copy
 * inside this function. This allows us not to copy Block unless we absolutely
 * have to.
 */
template <typename ReferenceType>
Block SquashingTransform::addImpl(ReferenceType input_block)
{
    /// End of input stream.
    if (!input_block)
        return finalizeBlock();

    /// Just read block is already enough.
    if (isEnoughSize(input_block))
    {
        /// If no accumulated data, return just read block.
        /// Cursors already stored in input_block.
        if (!accumulated_block)
            return std::move(input_block);

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        return finalizeBlock(std::move(input_block));
    }

    /// Accumulated block is already enough.
    if (isEnoughSize(accumulated_block))
        return finalizeBlock(std::move(input_block));

    append<ReferenceType>(std::move(input_block));

    if (isEnoughSize(accumulated_block))
        return finalizeBlock();

    /// Squashed block is not ready.
    return {};
}


template <typename ReferenceType>
void SquashingTransform::append(ReferenceType input_block)
{
    if (input_block.info.cursors.has_value())
        cursor_merger.add(std::move(input_block.info.cursors.value()));

    if (!accumulated_block)
    {
        accumulated_block = std::move(input_block);
        return;
    }

    assert(blocksHaveEqualStructure(input_block, accumulated_block));

    try
    {
        for (size_t i = 0, size = accumulated_block.columns(); i < size; ++i)
        {
            const auto source_column = input_block.getByPosition(i).column;

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
        cursor_merger.finalize();
        throw;
    }
}

Block SquashingTransform::finalizeBlock(Block new_data)
{
    auto new_cursors = std::move(new_data.info.cursors);

    Block to_return = std::move(new_data);
    std::swap(to_return, accumulated_block);

    if (cursor_merger.hasSome())
        to_return.info.cursors = cursor_merger.finalize();

    if (new_cursors.has_value())
        cursor_merger.add(new_cursors.value());

    return to_return;
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
