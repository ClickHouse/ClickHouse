#pragma once
#include "config.h"

#if USE_PARQUET

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <boost/noncopyable.hpp>
#include <IO/WithFileName.h>
#include <generated/parquet_types.h>

#if USE_AWS_S3
#include <IO/ReadBufferFromS3.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#endif

namespace ProfileEvents
{
    extern const Event ParquetMetadataCacheMisses;
    extern const Event ParquetMetadataCacheHits;
}

namespace DB
{

struct ParquetMetadataCacheKey
{
    String file_path;
    String etag;
    bool operator==(const ParquetMetadataCacheKey & other) const;
};

/// Hash function for ParquetMetadataCacheKey
struct ParquetMetadataCacheKeyHash
{
    size_t operator()(const ParquetMetadataCacheKey & key) const;
};

/// Cache cell containing Parquet metadata
struct ParquetMetadataCacheCell : private boost::noncopyable
{
    parquet::format::FileMetaData metadata;
    Int64 memory_bytes;
    explicit ParquetMetadataCacheCell(parquet::format::FileMetaData metadata_);
private:
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200;
    size_t calculateMemorySize() const;
};

/// Weight function for metadata cache
struct ParquetMetadataCacheWeightFunction
{
    size_t operator()(const ParquetMetadataCacheCell & cell) const;
};

/// Parquet metadata cache
class ParquetMetadataCache : public CacheBase<ParquetMetadataCacheKey, ParquetMetadataCacheCell, ParquetMetadataCacheKeyHash, ParquetMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<ParquetMetadataCacheKey, ParquetMetadataCacheCell, ParquetMetadataCacheKeyHash, ParquetMetadataCacheWeightFunction>;
    ParquetMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);
    static ParquetMetadataCacheKey createKey(const String & file_path, const String & file_attr);
    /// Get or load Parquet metadata with caching
    template <typename LoadFunc>
    parquet::format::FileMetaData getOrSetMetadata(const ParquetMetadataCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto metadata = load_fn();
            LOG_TRACE(log, "got metadata from cache {} | {}", key.file_path, key.etag);
            return std::make_shared<ParquetMetadataCacheCell>(std::move(metadata));
        };
        auto result = Base::getOrSet(key, load_fn_wrapper);
        if (result.second)
        {
            LOG_TRACE(log, "cache miss {} | {}", key.file_path, key.etag);
            ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheMisses);
        }
        else
        {
            LOG_TRACE(log, "cache hit {} | {}", key.file_path, key.etag);
            ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheHits);
        }
        return result.first->metadata;
    }

private:
    LoggerPtr log;
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using ParquetMetadataCachePtr = std::shared_ptr<ParquetMetadataCache>;
}
#endif
