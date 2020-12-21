#pragma once

#include <Common/ArrayCache.h>
#include <Common/SipHash.h>
#include <Common/UInt128.h>
#include <Common/ProfileEvents.h>
#include <IO/BufferWithOwnMemory.h>


namespace ProfileEvents
{
    extern const Event UncompressedCacheHits;
    extern const Event UncompressedCacheMisses;
}

namespace DB
{


struct UncompressedCacheCell
{
    size_t compressed_size;
    UInt32 additional_bytes;
};

/** Cache of decompressed blocks for implementation of CachedCompressedReadBuffer. thread-safe.
  */
class UncompressedCache : public ArrayCache<UInt128, UncompressedCacheCell>
{
private:
    using Base = ArrayCache<UInt128, UncompressedCacheCell>;

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

    HolderPtr get(const Key & key)
    {
        Base::HolderPtr res = Base::get(key);

        if (res)
            ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);

        return res;
    }

    template <typename GetSizeFunc, typename InitializeFunc>
    HolderPtr getOrSet(const Key & key, GetSizeFunc && get_size, InitializeFunc && initialize)
    {
        bool was_calculated = false;
        auto result = Base::getOrSet(key, get_size, initialize, &was_calculated);

        if (was_calculated)
            ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);

        return result;
    }

};

using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

}
