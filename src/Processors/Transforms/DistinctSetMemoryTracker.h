#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>

#include <atomic>
#include <memory>

namespace DB
{

/// Combined retained set memory when a parallel `DISTINCT` processed a chunk. Keeping the snapshot
/// lets the global limit check observe allocations even if a set is released before the chunk arrives.
struct DistinctSetMemoryUsage : ChunkInfoCloneable<DistinctSetMemoryUsage>
{
    explicit DistinctSetMemoryUsage(UInt64 total_bytes_) : total_bytes(total_bytes_)
    {
    }
    const UInt64 total_bytes;
};

/// Accounts for one transform's retained set memory in a counter shared by parallel final `DISTINCT`
/// transforms. Spilling and `LowCardinality` filtering can release memory without emitting new keys.
class DistinctSetMemoryTracker
{
public:
    using SharedCounter = std::shared_ptr<std::atomic<UInt64>>;

    explicit DistinctSetMemoryTracker(SharedCounter total_bytes_ = nullptr);
    ~DistinctSetMemoryTracker();

    DistinctSetMemoryTracker(const DistinctSetMemoryTracker &) = delete;
    DistinctSetMemoryTracker & operator=(const DistinctSetMemoryTracker &) = delete;

    /// Updates this transform's contribution and returns the combined memory at that point.
    UInt64 update(UInt64 bytes);

    /// Attaches a snapshot to result chunks, or emits an empty chunk if only set memory changed.
    /// A null shared counter disables global accounting and reporting.
    void report(Chunk & chunk, const Block & header, UInt64 bytes);

private:
    const SharedCounter total_bytes;
    UInt64 accounted_bytes = 0;
};

}
