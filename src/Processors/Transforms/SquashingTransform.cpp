#include <utility>
#include <Processors/Transforms/SquashingTransform.h>
#include <Columns/IColumn.h>
#include <Interpreters/Squashing.h>
#include <Processors/Chunk.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SquashingTransform::SquashingTransform(
    SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes,
    size_t max_block_size_rows, size_t max_block_size_bytes, bool squash_with_strict_limits)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(header, min_block_size_rows, min_block_size_bytes,
                max_block_size_rows, max_block_size_bytes, squash_with_strict_limits)
{
}

void SquashingTransform::onConsume(Chunk chunk)
{
    squashing.add(std::move(chunk));
}

SquashingTransform::GenerateResult SquashingTransform::onGenerate()
{
    cur_chunk = Squashing::squash(squashing.generate(), getInputPort().getSharedHeader());

    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = !canGenerate();
    return res;
}

bool SquashingTransform::canGenerate()
{
    return squashing.canGenerate();
}

void SquashingTransform::onFinish()
{
    finish_chunk = Squashing::squash(squashing.flush(), getInputPort().getSharedHeader());
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

namespace
{

/// `Chunk::bytes` with constant columns at their materialized size, as `Squashing::squash` materializes them.
size_t squashedBytes(const Chunk & chunk)
{
    size_t bytes = 0;
    for (const auto & column : chunk.getColumns())
        bytes += isColumnConst(*column) ? column->byteSizeAt(0) * column->size() : column->byteSize();
    return bytes;
}

}

SimpleSquashingChunksTransform::SimpleSquashingChunksTransform(
    SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes,
    size_t max_block_size_rows_, size_t max_block_size_bytes_)
    : IInflatingTransform(header, header)
    , squashing(header, min_block_size_rows, min_block_size_bytes)
    , max_block_size_rows(max_block_size_rows_)
    , max_block_size_bytes(max_block_size_bytes_)
{
}

void SimpleSquashingChunksTransform::consume(Chunk chunk)
{
    const size_t chunk_bytes = max_block_size_bytes && chunk.getNumRows() ? squashedBytes(chunk) : 0;
    const size_t buffered_rows = squashing.getRows();
    /// A generation round always empties `squashing`, so the buffered bytes are those added since it was last empty.
    if (!buffered_rows)
        buffered_bytes = 0;
    else if ((max_block_size_rows && buffered_rows + chunk.getNumRows() > max_block_size_rows)
             || (max_block_size_bytes && buffered_bytes + chunk_bytes > max_block_size_bytes))
    {
        flushed_chunk = Squashing::squash(squashing.flush(), getOutputPort().getSharedHeader());
        buffered_bytes = 0;
    }

    buffered_bytes += chunk_bytes;
    squashing.add(std::move(chunk));
}

Chunk SimpleSquashingChunksTransform::generate()
{
    if (flushed_chunk)
    {
        Chunk result;
        result.swap(flushed_chunk);
        return result;
    }

    squashed_chunk = Squashing::squash(squashing.generate(), getOutputPort().getSharedHeader());

    if (squashed_chunk.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    Chunk result;
    result.swap(squashed_chunk);
    return result;
}

bool SimpleSquashingChunksTransform::canGenerate()
{
    return static_cast<bool>(flushed_chunk) || squashing.canGenerate();
}

Chunk SimpleSquashingChunksTransform::getRemaining()
{
    chassert(!flushed_chunk);
    return Squashing::squash(squashing.flush(), getOutputPort().getSharedHeader());
}

}
