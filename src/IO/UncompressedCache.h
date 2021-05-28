#pragma once

#include <Common/LRUCache.h>
#include <Common/SipHash.h>
#include <Common/UInt128.h>
#include <Common/ProfileEvents.h>
#include <IO/BufferWithOwnMemory.h>


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
class UncompressedCache : public LRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>
{
private:
    using Base = LRUCache<UInt128, UncompressedCacheCell, UInt128TrivialHash, UncompressedSizeWeightFunction>;

public:
    UncompressedCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.update(offset);
        hash.get128(key.low, key.high);

        return key;
    }

    MappedPtr get(const Key & key)
    {
        MappedPtr res = Base::get(key);

        if (res)
            ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);

        return res;
    }

private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::UncompressedCacheWeightLost, weight_loss);
    }
};

using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

}
