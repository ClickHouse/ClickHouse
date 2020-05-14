#pragma once

#include <memory>

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

using MarkCacheBase = IGrabberAllocator<
    /* Key */ UInt128,
    /* Value */ CacheMarksInCompressedFile,
    /* Key hash */ UInt128TrivialHash>;

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

    //ValuePtr getOrSet(const Key & key, GAInitFunction auto && init_func)
    template <class SizeFunc, class InitFunc>
    ValuePtr getOrSet(const Key & key, SizeFunc && size_func, InitFunc && init_func)
    {
        auto&& [ptr, produced] = MarkCacheBase::getOrSet(key,
                std::forward<SizeFunc>(size_func),
                std::forward<InitFunc>(init_func));

        if (produced)
            ProfileEvents::increment(ProfileEvents::MarkCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::MarkCacheHits);

        return ptr;
    }
};

using MarkCachePtr = std::shared_ptr<MarkCache>;
}

