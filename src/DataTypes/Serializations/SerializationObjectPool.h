#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>

namespace CurrentMetrics
{
    extern const Metric SerializationCacheBytes;
    extern const Metric SerializationCacheCount;
}

namespace DB
{

/// Cache for constant serialization objects.
/// Deduplicates identical serializations so that concurrent users of the
/// same type share one object. Uses CacheBase (LRU) under the hood;
/// objects in active use are kept alive by external shared_ptr holders
/// even after the cache evicts them.
class SerializationObjectPool
{
public:
    static SerializationObjectPool & instance()
    {
        static SerializationObjectPool pool;
        return pool; 
    }

    SerializationPtr getOrCreate(UInt128 key, SerializationPtr && serialization)
    {
        auto [result, _] = cache.getOrSet(key, [&]() { return std::move(serialization); });
        return result;
    }

private:
    static constexpr size_t max_cache_size = 1000;

    SerializationObjectPool()
        : cache("LRU",
                CurrentMetrics::SerializationCacheBytes,
                CurrentMetrics::SerializationCacheCount,
                /*max_size_in_bytes=*/0,
                /*max_count=*/max_cache_size,
                /*size_ratio=*/ 0)
    {
    }

    CacheBase<UInt128, const ISerialization, UInt128Hash> cache;
};

}
