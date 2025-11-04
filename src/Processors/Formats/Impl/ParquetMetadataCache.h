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
    String etag;
    
    bool operator==(const ParquetMetadataCacheKey & other) const
    {
        return file_path == other.file_path && etag == other.etag;
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
        
        // Add schema size estimation
        if (metadata->schema())
        {
            size += metadata->schema()->ToString().size();
        }
        
        // Add row group metadata size estimation
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

/// Parquet metadata cache implementation
class ParquetMetadataCache : public CacheBase<ParquetMetadataCacheKey, ParquetMetadataCacheCell, std::hash<String>, ParquetMetadataCacheWeightFunction>
{
public:
    using Base = CacheBase<ParquetMetadataCacheKey, ParquetMetadataCacheCell, std::hash<String>, ParquetMetadataCacheWeightFunction>;

    ParquetMetadataCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::ParquetMetadataCacheBytes, CurrentMetrics::ParquetMetadataCacheFiles, max_size_in_bytes, max_count, size_ratio)
    {}

    static ParquetMetadataCacheKey createKey(const String & file_path, const String & etag)
    {
        return ParquetMetadataCacheKey{file_path, etag};
    }

    /// Get or load Parquet metadata with caching
    template <typename LoadFunc>
    std::shared_ptr<parquet::FileMetaData> getOrSetMetadata(const ParquetMetadataCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto metadata = load_fn();
            return std::make_shared<ParquetMetadataCacheCell>(metadata);
        };
        
        auto result = Base::getOrSet(key, load_fn_wrapper);
        
        if (result.second)
            ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheHits);
            
        return result.first->metadata;
    }

private:
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr) override
    {
        ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheWeightLost, weight_loss);
        UNUSED(mapped_ptr);
    }
};

using ParquetMetadataCachePtr = std::shared_ptr<ParquetMetadataCache>;

}

#endif
