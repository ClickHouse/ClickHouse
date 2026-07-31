#pragma once

#include <boost/functional/hash.hpp>
#include <boost/noncopyable.hpp>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <base/types.h>

#include <optional>

namespace ProfileEvents
{
extern const Event PuffinFilesCacheHits;
extern const Event PuffinFilesCacheMisses;
extern const Event PuffinFilesCacheWeightLost;
}

namespace DB
{

struct PuffinFilesCacheKey
{
    String file_path;
    String etag;
    Int64 content_offset = 0;
    Int64 content_size_in_bytes = 0;
    String referenced_data_file;
    /// Manifest DV record_count / expected roaring cardinality. Included so a cache hit cannot
    /// skip re-validation when a later request declares a different cardinality for the same slice.
    UInt64 expected_cardinality = 0;
    /// Data-file manifest record_count used to bound DV positions. Same rationale as cardinality.
    UInt64 data_file_record_count = 0;

    bool operator==(const PuffinFilesCacheKey & other) const;
};

struct PuffinFilesCacheKeyHash
{
    size_t operator()(const PuffinFilesCacheKey & key) const;
};

struct PuffinFilesCacheCell : private boost::noncopyable
{
    DataLakeObjectMetadata::ExcludedRowsPtr excluded_rows;
    bool is_empty_deletion_vector = false;
    UInt64 memory_bytes = 0;

    explicit PuffinFilesCacheCell(DataLakeObjectMetadata::ExcludedRowsPtr excluded_rows_);

private:
    static constexpr UInt64 EMPTY_DELETION_VECTOR_WEIGHT = 1;
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 200;

    static UInt64 calculateMemorySize(bool is_empty_deletion_vector_, const DataLakeObjectMetadata::ExcludedRowsPtr & excluded_rows_);
};

struct PuffinFilesCacheWeightFunction
{
    size_t operator()(const PuffinFilesCacheCell & cell) const;
};

/// Cache for parsed content loaded from Puffin files (deletion vectors today, indexes later).
class PuffinFilesCache : public CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>
{
public:
    using Base = CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>;

    PuffinFilesCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);

    static std::optional<PuffinFilesCacheKey> tryCreateKey(
        const String & file_path,
        const String & etag,
        Int64 content_offset,
        Int64 content_size_in_bytes,
        const String & referenced_data_file,
        UInt64 expected_cardinality,
        UInt64 data_file_record_count);

    template <typename LoadFunc>
    DataLakeObjectMetadata::ExcludedRowsPtr getOrSetDeletionVector(const PuffinFilesCacheKey & key, LoadFunc && load_fn)
    {
        /// True if this caller's load_fn ran. Needed together with `contains()` because
        /// CacheBase::getOrSet returns `{value, false}` for hits, for a clear()-discarded
        /// producer, and for stampede waiters of that discarded load.
        bool loaded = false;
        auto load_fn_wrapper = [&]()
        {
            loaded = true;
            auto excluded_rows = load_fn();
            const bool is_empty_deletion_vector = !excluded_rows;
            if (is_empty_deletion_vector)
            {
                LOG_TRACE(
                    log,
                    "Cached empty puffin deletion vector for {} | {} at offset {} length {} for data file {}",
                    key.file_path,
                    key.etag,
                    key.content_offset,
                    key.content_size_in_bytes,
                    key.referenced_data_file);
            }
            else
            {
                LOG_TRACE(
                    log,
                    "Loaded puffin deletion vector into cache for {} | {} at offset {} length {} for data file {}",
                    key.file_path,
                    key.etag,
                    key.content_offset,
                    key.content_size_in_bytes,
                    key.referenced_data_file);
            }
            return std::make_shared<PuffinFilesCacheCell>(std::move(excluded_rows));
        };

        auto result = Base::getOrSet(key, load_fn_wrapper);
        /// `result.second` means inserted. A concurrent clear() can leave producer and stampede
        /// waiters with a value that is not resident — those must count as misses, not hits.
        const bool served_from_cache = !result.second && !loaded && contains(key);
        if (!served_from_cache)
        {
            if (loaded && !result.second)
            {
                LOG_TRACE(
                    log,
                    "Puffin files cache miss (load discarded by concurrent clear) for {} | {} at offset {} length {} for data file {}",
                    key.file_path,
                    key.etag,
                    key.content_offset,
                    key.content_size_in_bytes,
                    key.referenced_data_file);
            }
            else if (!result.second && !loaded)
            {
                LOG_TRACE(
                    log,
                    "Puffin files cache miss (waited for load discarded by concurrent clear) for {} | {} at offset {} length {} for data file {}",
                    key.file_path,
                    key.etag,
                    key.content_offset,
                    key.content_size_in_bytes,
                    key.referenced_data_file);
            }
            else
            {
                LOG_TRACE(
                    log,
                    "Puffin files cache miss for {} | {} at offset {} length {} for data file {}",
                    key.file_path,
                    key.etag,
                    key.content_offset,
                    key.content_size_in_bytes,
                    key.referenced_data_file);
            }
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheMisses);
        }
        else if (result.first->is_empty_deletion_vector)
        {
            LOG_TRACE(log, "Puffin files cache hit (empty deletion vector) for {} | {} at offset {} length {} for data file {}", key.file_path, key.etag, key.content_offset, key.content_size_in_bytes, key.referenced_data_file);
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheHits);
        }
        else
        {
            LOG_TRACE(log, "Puffin files cache hit for {} | {} at offset {} length {} for data file {}", key.file_path, key.etag, key.content_offset, key.content_size_in_bytes, key.referenced_data_file);
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheHits);
        }

        return cloneExcludedRows(*result.first);
    }

private:
    static DataLakeObjectMetadata::ExcludedRowsPtr cloneExcludedRows(const PuffinFilesCacheCell & cell);

    LoggerPtr log;

    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using PuffinFilesCachePtr = std::shared_ptr<PuffinFilesCache>;

}
