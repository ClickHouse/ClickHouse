#pragma once

#include <memory>

#include <Common/CachingAllocator.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/AggregationCommon.h>
#include <DataStreams/MarkInCompressedFile.h>


namespace ProfileEvents
{
    extern const Event MarkCacheHits;
    extern const Event MarkCacheMisses;
}

namespace DB
{

/// Estimate of number of bytes in cache for marks.
struct MarksWeightFunction
{
    size_t operator()(const MarksInCompressedFile & marks) const
    {
        /// NOTE Could add extra 100 bytes for overhead of std::vector, cache structures and allocator.
        return marks.size() * sizeof(MarkInCompressedFile);
    }
};


/**
 * Cache of \c marks for \c StorageMergeTree.
 * \c Mark is an index structure that addresses column file ranges that correspond to primary key ranges.
 */
class MarkCache : public CachingAllocator<UInt128, MarksInCompressedFile, UInt128TrivialHash>
{
private:
    using Base = CachingAllocator<UInt128, MarksInCompressedFile, UInt128TrivialHash>;
public:
    using Base::get;

    explicit MarkCache(size_t max_size_in_bytes): Base(max_size_in_bytes) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String& path_to_file)
    {
        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);

        UInt128 key{};
        hash.get128(key.low, key.high);

        return key;
    }

    template <typename Init>
    inline MemoryRegionPtr getOrSet(const Key& key, Init&& initialize) {
        auto&& [region, was_produced] = Base::getOrSet(
            key,
            MarksWeightFunction{},
            std::forward<Init>(initialize));

        if (was_produced)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return region;
    }
};

using MarkCachePtr = std::shared_ptr<MarkCache>;
}
