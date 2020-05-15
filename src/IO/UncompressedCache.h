#pragma once

#include <Common/IGrabberAllocator.h>
#include <Common/SipHash.h>
#include <Common/UInt128.h>
#include <Common/ProfileEvents.h>
#include <IO/BufferWithOwnMemory.h>

#include <cstddef>

namespace ProfileEvents
{
    extern const Event UncompressedCacheHits;
    extern const Event UncompressedCacheMisses;
    extern const Event UncompressedCacheWeightLost;
}

namespace DB
{
struct UncompressedCell
{
    Memory<> data;
    size_t compressed_size;
    UInt32 additional_bytes;
    void * heap_storage{nullptr}; /// Needed to pass to CacheUncompressedCell.
};

struct CacheUncompressedCell
{
    Memory<FakeMemoryAllocForIG> data;
    size_t compressed_size;
    UInt32 additional_bytes;

    /// Called while initializing data in DB::IGrabberAllocator::RegionMetadata::init_value.
    CacheUncompressedCell(const UncompressedCell& other)
        : data(other.data, other.heap_storage), // calling copy-ctor with alloc params
          compressed_size(other.compressed_size),
          additional_bytes(other.additional_bytes) {}
};

using UncompressedCacheBase = IGrabberAllocator<UInt128, CacheUncompressedCell, UInt128TrivialHash>;

/**
 * Cache of decompressed blocks for implementation of CachedCompressedReadBuffer.
 */
class UncompressedCache : public UncompressedCacheBase
{
public:
    UncompressedCache(size_t max_size_in_bytes): UncompressedCacheBase(max_size_in_bytes) {}

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

    ValuePtr get(const Key & key)
    {
        ValuePtr ptr = UncompressedCacheBase::get(key);

        if (ptr)
            ProfileEvents::increment(ProfileEvents::UncompressedCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::UncompressedCacheMisses);

        return ptr;
    }
};
using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;
}

