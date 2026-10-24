#include <IO/NullWriteBuffer.h>
#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Port.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/Scheduler/CurrentCPULease.h>

#include <optional>


namespace DB
{

NullWriteBuffer LazyOutputFormat::out;

LazyOutputFormat::LazyOutputFormat(SharedHeader header)
    : IOutputFormat(header, out), queue(2)
{
}

Chunk LazyOutputFormat::getChunk(UInt64 milliseconds)
{
    if (isFinished())
        return {};

    Chunk chunk;
    {
        std::optional<CPULeaseParkGuard> park;
        if (queue.empty())
            park.emplace();

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
    }

    if (chunk)
        info.update(chunk.getNumRows(), chunk.allocatedBytes());

    return chunk;
}

void LazyOutputFormat::consume(Chunk chunk)
{
    /// This is the only producer, so a free place stays free.
    std::optional<CPULeaseParkGuard> park;
    if (queue.size() >= queue.maxFill())
        park.emplace();

    (void)(queue.emplace(std::move(chunk)));
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
