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
    extern const Event ParquetMetadataCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric ParquetMetadataCacheBytes;
    extern const Metric ParquetMetadataCacheFiles;
}

namespace DB
{

struct ParquetMetadataCacheKey
{
    String file_path;
    String file_attr;
    bool operator==(const ParquetMetadataCacheKey & other) const
    {
        return file_path == other.file_path && file_attr == other.file_attr;
    }
};

/// Hash function for ParquetMetadataCacheKey
struct ParquetMetadataCacheKeyHash
{
    size_t operator()(const ParquetMetadataCacheKey & key) const
    {
        return std::hash<String>{}(key.file_path + key.file_attr);
    }
};

/// Cache cell containing Parquet metadata
struct ParquetMetadataCacheCell : private boost::noncopyable
{
    std::shared_ptr<parquet::FileMetaData> metadata;
    Int64 memory_bytes;
    explicit ParquetMetadataCacheCell(std::shared_ptr<parquet::FileMetaData> metadata_)
        : metadata(std::move(metadata_))
        , memory_bytes(calculateMemorySize() + SIZE_IN_MEMORY_OVERHEAD)
    {
    }

private:
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200;

    size_t calculateMemorySize() const
    {
        if (!metadata)
            return 0;
        size_t size = sizeof(parquet::FileMetaData);
        /// Add schema size estimation
        if (metadata->schema())
        {
            size += metadata->schema()->ToString().size();
        }
        /// Add row group metadata size estimation
        for (int i = 0; i < metadata->num_row_groups(); ++i)
        {
            size += sizeof(parquet::RowGroupMetaData);
            auto rg = metadata->RowGroup(i);
            for (int j = 0; j < rg->num_columns(); ++j)
            {
                size += sizeof(parquet::ColumnChunkMetaData);
            }
        }
        return size;
    }
};

/// Weight function for cache eviction
struct ParquetMetadataCacheWeightFunction
{
    size_t operator()(const ParquetMetadataCacheCell & cell) const
    {
        return cell.memory_bytes;
    }
};

/// Parquet metadata cache
class ParquetMetadataCache : public CacheBase<ParquetMetadataCacheKey, ParquetMetadataCacheCell, ParquetMetadataCacheKeyHash, ParquetMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<ParquetMetadataCacheKey, ParquetMetadataCacheCell, ParquetMetadataCacheKeyHash, ParquetMetadataCacheWeightFunction>;

    ParquetMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::ParquetMetadataCacheBytes, CurrentMetrics::ParquetMetadataCacheFiles, max_size_in_bytes, max_count, size_ratio)
        , log(getLogger("ParquetMetadataCache"))
    {}

    static ParquetMetadataCacheKey createKey(const String & file_path, const String & file_attr)
    {
        return ParquetMetadataCacheKey{file_path, file_attr};
    }

    /// Get or load Parquet metadata with caching
    template <typename LoadFunc>
    std::shared_ptr<parquet::FileMetaData> getOrSetMetadata(const ParquetMetadataCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto metadata = load_fn();
            LOG_DEBUG(log, "got metadata from cache {} | {}", key.file_path, key.file_attr);
            return std::make_shared<ParquetMetadataCacheCell>(metadata);
        };
        auto result = Base::getOrSet(key, load_fn_wrapper);
        if (result.second)
        {
            LOG_DEBUG(log, "cache miss {} | {}", key.file_path, key.file_attr);
            ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheMisses);
        }
        else
        {
            LOG_DEBUG(log, "cache hit {} | {}", key.file_path, key.file_attr);
            ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheHits);
        }
        return result.first->metadata;
    }

private:
    LoggerPtr log;
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr) override
    {
        LOG_DEBUG(log, "cache evict {} | {}", mapped_ptr->metadata->path(), mapped_ptr->metadata->file_attributes());
        ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheWeightLost, weight_loss);
        UNUSED(mapped_ptr);
    }
};

/* Cache cell containing V3 native Parquet metadata
we store the native metadata here instead of a shared pointer
wrapping the metadata
*/
struct ParquetV3MetadataCacheCell : private boost::noncopyable
{
    parquet::format::FileMetaData metadata;
    Int64 memory_bytes;
    explicit ParquetV3MetadataCacheCell(parquet::format::FileMetaData metadata_)
        : metadata(std::move(metadata_))
        , memory_bytes(calculateMemorySize() + SIZE_IN_MEMORY_OVERHEAD)
    {
    }

private:
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200;

    size_t calculateMemorySize() const
    {
        /// Estimate memory usage of native metadata
        /// This is simpler than Arrow metadata since it's not a shared_ptr
        return sizeof(metadata) + metadata.schema.size() * 100; // Rough estimate
    }
};

/// Weight function for V3 metadata cache
struct ParquetV3MetadataCacheWeightFunction
{
    size_t operator()(const ParquetV3MetadataCacheCell & cell) const
    {
        return cell.memory_bytes;
    }
};

/// V3 Parquet metadata cache - reuses same key and hash as V2 cache
class ParquetV3MetadataCache : public CacheBase<ParquetMetadataCacheKey, ParquetV3MetadataCacheCell, ParquetMetadataCacheKeyHash, ParquetV3MetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<ParquetMetadataCacheKey, ParquetV3MetadataCacheCell, ParquetMetadataCacheKeyHash, ParquetV3MetadataCacheWeightFunction>;

    ParquetV3MetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::ParquetMetadataCacheBytes, CurrentMetrics::ParquetMetadataCacheFiles, max_size_in_bytes, max_count, size_ratio)
        , log(getLogger("ParquetV3MetadataCache"))
    {}

    /// Same factory method as V2 - reuse the key creation logic
    static ParquetMetadataCacheKey createKey(const String & file_path, const String & file_attr)
    {
        return ParquetMetadataCacheKey{file_path, file_attr};
    }

    /// Get or load V3 Parquet metadata with caching
    template <typename LoadFunc>
    parquet::format::FileMetaData getOrSetMetadata(const ParquetMetadataCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto metadata = load_fn();
            LOG_DEBUG(log, "got metadata from cache {} | {}", key.file_path, key.file_attr);
            return std::make_shared<ParquetV3MetadataCacheCell>(std::move(metadata));
        };
        auto result = Base::getOrSet(key, load_fn_wrapper);
        /// Reuse same ProfileEvents as V2 cache
        if (result.second)
        {
            LOG_DEBUG(log, "cache miss {} | {}", key.file_path, key.file_attr);
            ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheMisses);
        }
        else
        {
            LOG_DEBUG(log, "cache hit {} | {}", key.file_path, key.file_attr);
            ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheHits);
        }
        return result.first->metadata;
    }

private:
    LoggerPtr log;
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr) override
    {
        LOG_DEBUG(log, "cache evict {} | {}", mapped_ptr->metadata.path(), mapped_ptr->metadata.file_attributes());
        ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheWeightLost, weight_loss);
        UNUSED(mapped_ptr);
    }
};

using ParquetMetadataCachePtr = std::shared_ptr<ParquetMetadataCache>;
using ParquetV3MetadataCachePtr = std::shared_ptr<ParquetV3MetadataCache>;

std::pair<String, String> extractObjectAttributes(ReadBuffer & in);
std::optional<ObjectMetadata> tryGetObjectMetadata(ReadBuffer & in);

}

#endif
