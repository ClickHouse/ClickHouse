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

    Port::Data data;
    if (!queue.tryPop(data, milliseconds))
        return {};

    if (!data.exception)
        info.update(data.chunk.getNumRows(), data.chunk.allocatedBytes());

    return data.getChunkOrTrow();
}

Chunk LazyOutputFormat::getTotals()
{
    return totals.getChunkOrTrow();
}

Chunk LazyOutputFormat::getExtremes()
{
    return extremes.getChunkOrTrow();
}

void LazyOutputFormat::setRowsBeforeLimit(size_t rows_before_limit)
{
    info.setRowsBeforeLimit(rows_before_limit);
}

}
