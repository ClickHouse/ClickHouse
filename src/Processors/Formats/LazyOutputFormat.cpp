#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{

WriteBuffer LazyOutputFormat::out(nullptr, 0);

Chunk LazyOutputFormat::getChunk(UInt64 milliseconds)
{
    if (finished_processing)
    {
        if (queue.empty())
            return {};
    }

    Chunk chunk;
    if (!queue.tryPop(chunk, milliseconds))
        return {};

    if (chunk)
        info.update(chunk.getNumRows(), chunk.allocatedBytes());

    return chunk;
}

Chunk LazyOutputFormat::getTotals()
{
    return std::move(totals);
}

Chunk LazyOutputFormat::getExtremes()
{
    return std::move(extremes);
}

void LazyOutputFormat::setRowsBeforeLimit(size_t rows_before_limit)
{
    info.setRowsBeforeLimit(rows_before_limit);
}

}
