#pragma once

#include <memory>

#include <list>
#include <Core/Block.h>
#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/AggregationCommon.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <DataStreams/BlocksListBlockInputStream.h>

namespace ProfileEvents
{
    extern const Event QueryCacheHits;
    extern const Event QueryCacheMisses;
}



namespace DB
{

/// Estimate of number of bytes in cache for marks.
struct QueryCacheWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t QUERY_CACHE_OVERHEAD = 128;

    size_t operator()(const QueryCacheMapped & query_cache_mapped) const
    {
        size_t size_in_bytes = 0;
        for (const auto & block : query_cache_mapped.getBlocks()) {
            size_in_bytes += block.bytes();
        }
        return size_in_bytes + QUERY_CACHE_OVERHEAD;
    }
};


class QueryCacheMapped 
{
public:
    QueryCacheMapped();
    const BlocksList& getBlocks() const
    {
        return blocks_;
    }
    BlocksListBlockInputStream getStream()
    {
        return {blocks_.begin(), blocks_.end()};
    }

private:
    BlocksList blocks_;
    UInt32 accessed_ms;
    UInt32 expires_ms;
};


class QueryCache : public LRUCache<UInt128, QueryCacheMapped, UInt128TrivialHash, QueryCacheWeightFunction>
{
private:
    using Base = LRUCache<UInt128, QueryCacheMapped, UInt128TrivialHash, QueryCacheWeightFunction>;

public:
    QueryCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes) {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.get128(key.low, key.high);

        return key;
    }

    template <typename LoadFunc>
    BlocksListBlockInputStream getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::QueryCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::QueryCacheMisses);

        // auto mapped_ptr = result.first;

        // BlocksListBlockInputStream();

        return result.first;
    }
};

using QueryCachePtr = std::shared_ptr<QueryCache>;

}
