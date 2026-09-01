#pragma once

#include <Common/ProfileEvents.h>
#include <Common/HashTable/Hash.h>
#include <Common/JemallocCacheAllocator.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/CacheBase.h>


namespace ProfileEvents
{
    extern const Event UncompressedCacheHits;
    extern const Event UncompressedCacheMisses;
    extern const Event UncompressedCacheWeightLost;
}

namespace DB
{


/// Allocates the cache entry itself the way its data is allocated. The cache owns the entry, so creating it must
/// not be charged to whichever query filled it, and dropping the last reference must not credit whichever query
/// happened to hold it last. Covers the control block too, which `std::allocate_shared` puts in the same block.
template <typename T>
struct ServerOwnedCacheEntryAllocator
{
    using value_type = T;

    ServerOwnedCacheEntryAllocator() = default;
    template <typename U>
    explicit ServerOwnedCacheEntryAllocator(const ServerOwnedCacheEntryAllocator<U> &) {}

    T * allocate(size_t n)
    {
        MemoryTrackerBlockerInThread cache_entry_not_charged_to_the_query;
        return static_cast<T *>(::operator new(n * sizeof(T)));
    }

    void deallocate(T * p, size_t n) noexcept
    {
        MemoryTrackerBlockerInThread cache_entry_not_charged_to_the_query;
        ::operator delete(p, n * sizeof(T));
    }

    template <typename U>
    bool operator==(const ServerOwnedCacheEntryAllocator<U> &) const { return true; }
};

struct UncompressedCacheCell
{
    Memory<JemallocCacheAllocator> data;
    size_t compressed_size{};
    UInt32 additional_bytes{};

    /// `data` was allocated without charging the query, see `CachedCompressedReadBuffer::nextImpl`; release it
    /// the same way so eviction never gets credited to whichever query triggers it.
    ~UncompressedCacheCell()
    {
        MemoryTrackerBlockerInThread cached_bytes_not_charged_to_the_query;
        data = {};
    }
};

struct UncompressedSizeWeightFunction
{
    size_t operator()(const UncompressedCacheCell & x) const
    {
        return x.data.size();
    }
};

extern template class CacheBase<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

/** Cache of decompressed blocks for implementation of CachedCompressedReadBuffer. thread-safe.
  */
class UncompressedCache : public CacheBase<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>
{
private:
    using Base = CacheBase<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

public:
    UncompressedCache(const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        double size_ratio);

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset);

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, std::forward<LoadFunc>(load));

        if (result.second)
            ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);

        return result.first;
    }

private:
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr) override
    {
        ProfileEvents::increment(ProfileEvents::UncompressedCacheWeightLost, weight_loss);
        UNUSED(mapped_ptr);
    }
};

using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

}
