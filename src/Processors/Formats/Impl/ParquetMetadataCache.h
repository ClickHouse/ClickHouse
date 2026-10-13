#pragma once
#include "config.h"

#if USE_PARQUET

#include <boost/functional/hash.hpp>
#include <Core/Range.h>
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

#include <mutex>
#include <unordered_map>

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

/// Min/max statistics ranges of filter columns, kept with the cached metadata of a file version.
struct ParquetKeptStatistics : private boost::noncopyable
{
    /// The ranges of one column for every non-empty row group of the file, in the type of the column.
    using Ranges = std::vector<Range>;
    using RangesPtr = std::shared_ptr<const Ranges>;

    struct Column
    {
        String key;
        RangesPtr ranges;
        size_t bytes = 0;
    };

    explicit ParquetKeptStatistics(size_t max_bytes_) : max_bytes(max_bytes_) {}

    /// A copy of `ranges` to `add`, or nothing if ranges are already kept under `column_key` or all kept ranges would
    /// exceed `max_bytes`.
    std::optional<Column> prepare(const String & column_key, const Ranges & ranges) const;
    /// Returns false if ranges are already kept under the key or all kept ranges would exceed `max_bytes`.
    bool add(Column column);
    RangesPtr get(const String & column_key) const;
    size_t bytes() const;

private:
    const size_t max_bytes;
    mutable std::mutex mutex;
    std::unordered_map<String, RangesPtr> ranges_by_column TSA_GUARDED_BY(mutex);
    size_t total_bytes TSA_GUARDED_BY(mutex) = 0;
};

/// Cache cell containing Parquet metadata
struct ParquetMetadataCacheCell : private boost::noncopyable
{
    parquet::format::FileMetaData metadata;
    UInt64 memory_bytes;
    /// Counts in the weight of the cell (see `ParquetMetadataCache::keepStatistics`).
    std::shared_ptr<ParquetKeptStatistics> kept_statistics;
    explicit ParquetMetadataCacheCell(parquet::format::FileMetaData metadata_);
private:
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200;
    static constexpr size_t MIN_KEPT_STATISTICS_BYTES = 4096;
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
        return getOrSetMetadataCell(key, std::forward<LoadFunc>(load_fn))->metadata;
    }

    template <typename LoadFunc>
    MappedPtr getOrSetMetadataCell(const ParquetMetadataCacheKey & key, LoadFunc && load_fn)
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
        return result.first;
    }

    /// Keeps `ranges` in `cell` and counts them in its weight, if the cache still holds `cell` under `key`.
    /// Moves the cell in the cache like a hit.
    void keepStatistics(
        const ParquetMetadataCacheKey & key, const MappedPtr & cell, const String & column_key, const ParquetKeptStatistics::Ranges & ranges);

private:
    LoggerPtr log;
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using ParquetMetadataCachePtr = std::shared_ptr<ParquetMetadataCache>;
}
#endif
