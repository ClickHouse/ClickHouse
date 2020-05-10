#pragma once

#include <memory>

#include <Common/LRUCache.h>
#include <Common/IGrabberAllocator.h>
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

using MarkCacheBase = IGrabberAllocator<
    /* Key */ UInt128,
    /* Value */ MarksInCompressedFile,
    /* Key hash */ UInt128TrivialHash,
    /* Value Size func */ MarksWeightFunction>;

/**
 * @brief Cache of marks for StorageMergeTree.
 *
 * Mark is an index structure that addresses ranges in column file corresponding to ranges of primary key.
 */
class MarkCache : public MarkCacheBase
{
public:
    using MarkCacheBase::ValuePtr;

    constexpr explicit MarkCache(size_t max_size_in_bytes): MarkCacheBase(max_size_in_bytes) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file) noexcept
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.get128(key.low, key.high);

        return key;
    }

    ValuePtr getOrSet(const Key & key, GAInitFunction auto && init_func)
    {
        auto result = MarkCacheBase::getOrSet(key, init_func);

        if (result.second)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return result.first;
    }
};

using MarkCachePtr = std::shared_ptr<MarkCache>;
}

