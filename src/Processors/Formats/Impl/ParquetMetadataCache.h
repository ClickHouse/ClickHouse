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
    parquet::format::FileMetaData metadata;
    Int64 memory_bytes;
    explicit ParquetMetadataCacheCell(parquet::format::FileMetaData metadata_)
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

/// Weight function for metadata cache
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
    parquet::format::FileMetaData getOrSetMetadata(const ParquetMetadataCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto metadata = load_fn();
            LOG_DEBUG(log, "got metadata from cache {} | {}", key.file_path, key.file_attr);
            return std::make_shared<ParquetMetadataCacheCell>(std::move(metadata));
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
    void onEntryRemoval(const size_t weight_loss, const MappedPtr &) override
    {
        LOG_DEBUG(log, "cache eviction");
        ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheWeightLost, weight_loss);
    }
};

using ParquetMetadataCachePtr = std::shared_ptr<ParquetMetadataCache>;

std::pair<String, String> extractObjectAttributes(ReadBuffer & in);

}

#endif
