#include <Processors/Transforms/DistinctSetMemoryTracker.h>

#include <Core/Block.h>

namespace DB
{

DistinctSetMemoryTracker::DistinctSetMemoryTracker(SharedCounter total_bytes_)
    : total_bytes(std::move(total_bytes_))
{
}

DistinctSetMemoryTracker::~DistinctSetMemoryTracker()
{
    update(0);
}

UInt64 DistinctSetMemoryTracker::update(UInt64 bytes)
{
    if (!total_bytes)
        return 0;

    UInt64 total = 0;
    if (bytes >= accounted_bytes)
    {
        const UInt64 delta = bytes - accounted_bytes;
        total = total_bytes->fetch_add(delta, std::memory_order_relaxed) + delta;
    }
    else
    {
        const UInt64 delta = accounted_bytes - bytes;
        total = total_bytes->fetch_sub(delta, std::memory_order_relaxed) - delta;
    }
    accounted_bytes = bytes;
    return total;
}

void DistinctSetMemoryTracker::report(Chunk & chunk, const Block & header, UInt64 bytes)
{
    if (!total_bytes)
        return;

    const bool changed = bytes != accounted_bytes;
    const UInt64 total = update(bytes);
    if (!chunk && !changed)
        return;

    if (!chunk)
        chunk = Chunk(header.cloneEmptyColumns(), 0);
    chunk.getChunkInfos().add(std::make_shared<DistinctSetMemoryUsage>(total));
}

}
