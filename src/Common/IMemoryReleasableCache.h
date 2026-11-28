#pragma once

#include <base/types.h>

namespace DB
{

/// A cache that gives its memory back when the server is short of it.
///
/// The memory of a cache counts against `max_server_memory_usage` like the memory of the queries
/// does, so a cache sized as a fraction of the memory of the server takes that fraction away from
/// the queries, and on a server whose queries use most of its memory they fail where they succeeded
/// without the cache. A cache implementing this interface is asked periodically by `MemoryWorker`,
/// and by `MemoryTracker` when an allocation is about to exceed the limit, to shrink to what fits
/// next to the rest of the server, and grows back once the rest needs less. See `PageCache::autoResize`
/// for the first cache doing so.
class IMemoryReleasableCache
{
public:
    virtual ~IMemoryReleasableCache() = default;

    /// Shrink so that `memory_usage`, which includes the cache, fits `memory_limit` - or grow back
    /// towards the configured size if it leaves room. Returns true if the usage fits the limit after
    /// the resize.
    virtual bool autoResize(Int64 memory_usage, size_t memory_limit) = 0;
};

/// The cache the global `MemoryTracker` asks to release memory when an allocation is about to exceed
/// the limit, if any. The pointer is not owned: whoever registers a cache unregisters it (with nullptr)
/// before the cache is destroyed.
void setMemoryReleasableCache(IMemoryReleasableCache * cache);
IMemoryReleasableCache * getMemoryReleasableCache();

}
