#pragma once
#include "config.h"

#if USE_PARQUET

#include <atomic>
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
    UInt64 memory_bytes;
    explicit ParquetMetadataCacheCell(parquet::format::FileMetaData metadata_);

    /// `computeParquetFooterDigest(metadata)`, computed on first use and then reused. The digest
    /// hashes every column chunk of every row group including the per-column statistics, so on a
    /// wide file it costs milliseconds, and a split read otherwise recomputes it once for the
    /// decision plus once per source when it writes the query condition cache. `metadata` is
    /// immutable for the cell's lifetime, so the value is stable.
    ///
    /// Computed lazily rather than in the constructor so a cache entry that never takes part in a
    /// split read does not pay for it at all.
    UInt64 footerDigest() const;

private:
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200;
    size_t calculateMemorySize() const;

    /// 0 means "not computed yet". A footer whose digest genuinely hashes to 0 is simply recomputed
    /// every time - correct, just not cached, and 0 is already the "no digest" sentinel in
    /// `ParquetFileBucketInfo::footer_digest`.
    mutable std::atomic<UInt64> memoized_footer_digest{0};
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
    /// Get or load the cache cell holding the Parquet metadata. Prefer this over `getOrSetMetadata`
    /// when the metadata is only read: `FileMetaData` owns a `ColumnChunk` per column per row group,
    /// each with its own vectors and statistics strings, so handing it back by value deep-copies the
    /// entire footer - milliseconds on a wide file with many row groups.
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

    /// Get or load Parquet metadata with caching, as an owned copy. For callers that keep the
    /// metadata past the lifetime of the cache entry.
    template <typename LoadFunc>
    parquet::format::FileMetaData getOrSetMetadata(const ParquetMetadataCacheKey & key, LoadFunc && load_fn)
    {
        return getOrSetMetadataCell(key, std::forward<LoadFunc>(load_fn))->metadata;
    }

private:
    LoggerPtr log;
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using ParquetMetadataCachePtr = std::shared_ptr<ParquetMetadataCache>;
}
#endif
