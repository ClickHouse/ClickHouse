#include <Processors/Formats/LazyOutputFormat.h>

namespace DB
{

WriteBuffer LazyOutputFormat::out(nullptr, 0);

Block LazyOutputFormat::getBlock(UInt64 milliseconds)
{
    if (finished)
    {
        if (queue.size() == 0)
            return {};
    }

    Chunk chunk;
    if (!queue.tryPop(chunk, milliseconds))
        return {};

    if (!chunk)
        return {};

    auto block = getPort(PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());
    info.update(block);

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

}
