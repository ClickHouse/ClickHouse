#pragma once

#include <Common/ProfileEvents.h>
#include <Common/HashTable/Hash.h>
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


struct UncompressedCacheCell
{
    Memory<> data;
    size_t compressed_size;
    UInt32 additional_bytes;
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
