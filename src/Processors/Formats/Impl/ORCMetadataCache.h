#pragma once
#include "config.h"

#if USE_ORC

#include <boost/functional/hash.hpp>
#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>
#include <boost/noncopyable.hpp>
#include <IO/WithFileName.h>

namespace ProfileEvents
{
    extern const Event ORCMetadataCacheMisses;
    extern const Event ORCMetadataCacheHits;
}

namespace DB
{

struct ORCMetadataCacheKey
{
    String file_path;
    String etag;
    bool operator==(const ORCMetadataCacheKey & other) const;
};

/// Hash function for ORCMetadataCacheKey
struct ORCMetadataCacheKeyHash
{
    size_t operator()(const ORCMetadataCacheKey & key) const;
};

/// Cache cell containing the serialized ORC file tail (PostScript + Footer + Metadata).
struct ORCMetadataCacheCell : private boost::noncopyable
{
    String serialized_tail;
    UInt64 memory_bytes;
    explicit ORCMetadataCacheCell(String serialized_tail_);
private:
    /// Rough per-entry overhead (in bytes) added on top of `serialized_tail.capacity()` so the
    /// cache weight also accounts for memory that the payload size does not capture: the key
    /// strings (`file_path` and `etag`, the former can be a long object-storage path), the cell
    /// and shared_ptr control block, and the cache's internal list/hash-map nodes. It is a
    /// heuristic constant (inherited from the Parquet metadata cache), not an exact figure; it
    /// mainly keeps very small entries from being weighted as nearly free.
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200;
};

/// Weight function for metadata cache
struct ORCMetadataCacheWeightFunction
{
    size_t operator()(const ORCMetadataCacheCell & cell) const;
};

/// ORC metadata cache
class ORCMetadataCache : public CacheBase<ORCMetadataCacheKey, ORCMetadataCacheCell, ORCMetadataCacheKeyHash, ORCMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<ORCMetadataCacheKey, ORCMetadataCacheCell, ORCMetadataCacheKeyHash, ORCMetadataCacheWeightFunction>;
    ORCMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);
    static ORCMetadataCacheKey createKey(const String & file_path, const String & file_attr);
    /// Get or load the serialized ORC file tail with caching
    template <typename LoadFunc>
    String getOrSetMetadata(const ORCMetadataCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto serialized_tail = load_fn();
            return std::make_shared<ORCMetadataCacheCell>(std::move(serialized_tail));
        };
        auto result = Base::getOrSet(key, load_fn_wrapper);
        if (result.second)
        {
            LOG_TRACE(log, "cache miss {} | {}", key.file_path, key.etag);
            ProfileEvents::increment(ProfileEvents::ORCMetadataCacheMisses);
        }
        else
        {
            LOG_TRACE(log, "cache hit {} | {}", key.file_path, key.etag);
            ProfileEvents::increment(ProfileEvents::ORCMetadataCacheHits);
        }
        return result.first->serialized_tail;
    }

private:
    LoggerPtr log;
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using ORCMetadataCachePtr = std::shared_ptr<ORCMetadataCache>;
}
#endif
