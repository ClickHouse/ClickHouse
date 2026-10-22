#pragma once
#include "config.h"

#if USE_PARQUET

#include <boost/functional/hash.hpp>
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

namespace DB::Parquet
{
struct FileFooter;
struct FileColumnChunks;
using FileColumnChunksPtr = std::shared_ptr<const FileColumnChunks>;
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

/// Cache cell containing Parquet metadata. Immutable: adding column chunks replaces the cell.
struct ParquetMetadataCacheCell : private boost::noncopyable
{
    std::shared_ptr<const Parquet::FileFooter> footer;
    /// By Parquet column index, nullptr for the columns not parsed yet; parsed from `footer`.
    std::vector<Parquet::FileColumnChunksPtr> column_chunks;
    UInt64 memory_bytes;
    explicit ParquetMetadataCacheCell(
        std::shared_ptr<const Parquet::FileFooter> footer_, std::vector<Parquet::FileColumnChunksPtr> column_chunks_ = {});
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
    /// Get or load Parquet metadata with caching. Returns the cell and whether it was a cache hit.
    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSetMetadata(const ParquetMetadataCacheKey & key, LoadFunc && load_fn)
    {
        auto load_fn_wrapper = [&]()
        {
            auto footer = load_fn();
            LOG_TRACE(log, "got metadata from cache {} | {}", key.file_path, key.etag);
            return std::make_shared<ParquetMetadataCacheCell>(std::move(footer));
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
        return {result.first, !result.second};
    }

    /// Replaces `cell`, as returned by getOrSetMetadata, with a cell that also has the non-null
    /// `column_chunks` it lacks. Does nothing if the cache no longer holds `cell` under `key`.
    void addColumnChunks(
        const ParquetMetadataCacheKey & key, const MappedPtr & cell, const std::vector<Parquet::FileColumnChunksPtr> & column_chunks);

private:
    LoggerPtr log;
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using ParquetMetadataCachePtr = std::shared_ptr<ParquetMetadataCache>;
}
#endif
