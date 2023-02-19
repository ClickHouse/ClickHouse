#pragma once

#include <Common/SipHash.h>
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


/** Cache of decompressed blocks for implementation of CachedCompressedReadBuffer. thread-safe.
  */
class UncompressedCache : public CacheBase<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>
{
private:
    using Base = CacheBase<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

public:
    explicit UncompressedCache(size_t max_size_in_bytes, const String & uncompressed_cache_policy = "")
        : Base(max_size_in_bytes, 0, uncompressed_cache_policy) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.update(offset);
        hash.get128(key);

        return key;
    }

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
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::UncompressedCacheWeightLost, weight_loss);
    }
};

using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

}
