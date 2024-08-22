#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{

WriteBufferFromPointer LazyOutputFormat::out(nullptr, 0);

Chunk LazyOutputFormat::getChunk(UInt64 milliseconds)
{
    if (isFinished())
        return {};

    Chunk chunk;
    if (milliseconds)
    {
        if (!queue.tryPop(chunk, milliseconds))
            return {};
    }
    else
    {
        if (!queue.pop(chunk))
            return {};
    }

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

void LazyOutputFormat::setRowsBeforeAggregation(size_t rows_before_aggregation)
{
    info.setRowsBeforeAggregation(rows_before_aggregation);
}
}
