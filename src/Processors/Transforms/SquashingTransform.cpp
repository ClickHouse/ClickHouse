#include <Processors/Transforms/SquashingTransform.h>
#include <Interpreters/Squashing.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

SquashingTransform::SquashingTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(header, min_block_size_rows, min_block_size_bytes)
{
}

void SquashingTransform::onConsume(Chunk chunk)
{
    Chunk planned_chunk = squashing.add(std::move(chunk));
    if (planned_chunk.hasChunkInfo())
        cur_chunk = DB::Squashing::squash(std::move(planned_chunk));
}

SquashingTransform::GenerateResult SquashingTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

void SquashingTransform::onFinish()
{
    Chunk chunk = squashing.flush();
    if (chunk.hasChunkInfo())
        chunk = DB::Squashing::squash(std::move(chunk));
    finish_chunk.setColumns(chunk.getColumns(), chunk.getNumRows());
}

void SquashingTransform::work()
{
    if (stage == Stage::Exception)
    {
        data.chunk.clear();
        ready_input = false;
        return;
    }

    ExceptionKeepingTransform::work();
    if (finish_chunk)
    {
        data.chunk = std::move(finish_chunk);
        ready_output = true;
    }
}

SimpleSquashingChunksTransform::SimpleSquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : IInflatingTransform(header, header), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::consume(Chunk chunk)
{
    Block current_block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    squashed_chunk.setColumns(current_block.getColumns(), current_block.rows());
}

Chunk SimpleSquashingChunksTransform::generate()
{
    if (squashed_chunk.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    return std::move(squashed_chunk);
}

bool SimpleSquashingChunksTransform::canGenerate()
{
    return !squashed_chunk.empty();
}

Chunk SimpleSquashingChunksTransform::getRemaining()
{
    Block current_block = squashing.add({});
    squashed_chunk.setColumns(current_block.getColumns(), current_block.rows());
    return std::move(squashed_chunk);
}

SquashingLegacy::SquashingLegacy(size_t min_block_size_rows_, size_t min_block_size_bytes_)
    : min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
{
}

Block SquashingLegacy::add(Block && input_block)
{
    return addImpl<Block &&>(std::move(input_block));
}

Block SquashingLegacy::add(const Block & input_block)
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
Block SquashingLegacy::addImpl(ReferenceType input_block)
{
    /// End of input stream.
    if (!input_block)
    {
        Block to_return;
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    /// Just read block is already enough.
    if (isEnoughSize(input_block))
    {
        /// If no accumulated data, return just read block.
        if (!accumulated_block)
        {
            return std::move(input_block);
        }

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        Block to_return = std::move(input_block);
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    /// Accumulated block is already enough.
    if (isEnoughSize(accumulated_block))
    {
        /// Return accumulated data and place new block to accumulated data.
        Block to_return = std::move(input_block);
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    append<ReferenceType>(std::move(input_block));
    if (isEnoughSize(accumulated_block))
    {
        Block to_return;
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    /// Squashed block is not ready.
    return {};
}


template <typename ReferenceType>
void SquashingLegacy::append(ReferenceType input_block)
{
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
        throw;
    }
}


bool SquashingLegacy::isEnoughSize(const Block & block)
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


bool SquashingLegacy::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}


}
