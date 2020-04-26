#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{

WriteBuffer LazyOutputFormat::out(nullptr, 0);

Block LazyOutputFormat::getBlock(UInt64 milliseconds)
{
    if (finished_processing)
    {
        if (queue.empty())
            return {};
    }

    Chunk chunk;
    if (!queue.tryPop(chunk, milliseconds))
        return {};

    if (!chunk)
        return {};

    auto block = getPort(PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());
    info.update(block);

    if (auto chunk_info = chunk.getChunkInfo())
    {
        if (const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(chunk_info.get()))
        {
            block.info.bucket_num = agg_info->bucket_num;
            block.info.is_overflows = agg_info->is_overflows;
        }
    }

    return block;
}

Block LazyOutputFormat::getTotals()
{
    if (!totals)
        return {};

    return getPort(PortKind::Totals).getHeader().cloneWithColumns(totals.detachColumns());
}

Block LazyOutputFormat::getExtremes()
{
    if (!extremes)
        return {};

    return getPort(PortKind::Extremes).getHeader().cloneWithColumns(extremes.detachColumns());
}

void LazyOutputFormat::setRowsBeforeLimit(size_t rows_before_limit)
{
    info.setRowsBeforeLimit(rows_before_limit);
}

}
