#pragma once

#include <boost/functional/hash.hpp>
#include <boost/noncopyable.hpp>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>
#include <base/types.h>

#include <memory>
#include <optional>
#include <vector>

namespace ProfileEvents
{
extern const Event PuffinFilesCacheHits;
extern const Event PuffinFilesCacheMisses;
extern const Event PuffinFilesCacheWeightLost;
extern const Event PuffinFooterCacheHits;
extern const Event PuffinFooterCacheMisses;
extern const Event PuffinFooterCacheWeightLost;
}

namespace DB
{

class IObjectStorage;

struct PuffinFilesCacheKey
{
    /// Distinguishes object-storage backends that share the same relative path (and possibly etag).
    /// Built via `makeStorageIdentity` from storage type + description (endpoint) + namespace + prefix.
    String storage_identity;
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

/// File-level footer identity (shared by all DV slices in one coalesced Puffin).
struct PuffinFooterCacheKey
{
    String storage_identity;
    String file_path;
    String etag;

    bool operator==(const PuffinFooterCacheKey & other) const;
};

struct PuffinFooterCacheKeyHash
{
    size_t operator()(const PuffinFooterCacheKey & key) const;
};

struct PuffinFooterCacheCell : private boost::noncopyable
{
    using BlobsPtr = std::shared_ptr<const std::vector<PuffinBlob>>;

    BlobsPtr blobs;
    UInt64 memory_bytes = 0;

    explicit PuffinFooterCacheCell(BlobsPtr blobs_);

    static UInt64 calculateMemorySize(const BlobsPtr & blobs_);
};

struct PuffinFooterCacheWeightFunction
{
    size_t operator()(const PuffinFooterCacheCell & cell) const;
};

class PuffinFooterCache
    : public CacheBase<PuffinFooterCacheKey, PuffinFooterCacheCell, PuffinFooterCacheKeyHash, PuffinFooterCacheWeightFunction>
{
public:
    using Base = CacheBase<PuffinFooterCacheKey, PuffinFooterCacheCell, PuffinFooterCacheKeyHash, PuffinFooterCacheWeightFunction>;

    PuffinFooterCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);

private:
    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

/// Cache for parsed content loaded from Puffin files (deletion vectors today, indexes later).
/// Also owns a sibling footer cache so coalesced multi-DV Puffins parse the footer once per file.
class PuffinFilesCache : public CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>
{
public:
    using Base = CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>;
    using FooterBlobsPtr = PuffinFooterCacheCell::BlobsPtr;

    PuffinFilesCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio);

    /// Stable backend identity for cache keys:
    /// `getName()://getDescription()/getObjectsNamespace()/getCommonKeyPrefix()`.
    static String makeStorageIdentity(const IObjectStorage & object_storage);

    static std::optional<PuffinFilesCacheKey> tryCreateKey(
        const String & storage_identity,
        const String & file_path,
        const String & etag,
        Int64 content_offset,
        Int64 content_size_in_bytes,
        const String & referenced_data_file,
        UInt64 expected_cardinality,
        UInt64 data_file_record_count);

    static std::optional<PuffinFooterCacheKey> tryCreateFooterKey(
        const String & storage_identity,
        const String & file_path,
        const String & etag);

    /// Clears deletion-vector and footer entries.
    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    void setMaxCount(size_t max_count);

    template <typename LoadFunc>
    FooterBlobsPtr getOrSetFooter(const PuffinFooterCacheKey & key, LoadFunc && load_fn)
    {
        bool loaded = false;
        auto load_fn_wrapper = [&]()
        {
            loaded = true;
            auto blobs = load_fn();
            LOG_TRACE(
                log,
                "Loaded puffin footer into cache for {} | {} | {} ({} blobs)",
                key.storage_identity,
                key.file_path,
                key.etag,
                blobs ? blobs->size() : 0);
            return std::make_shared<PuffinFooterCacheCell>(std::move(blobs));
        };

        auto [cell, outcome] = footer_cache.getOrSetWithOutcome(key, load_fn_wrapper);
        const bool served_from_cache = outcome == CacheGetOrSetOutcome::Hit;
        if (!served_from_cache)
        {
            LOG_TRACE(
                log,
                "Puffin footer cache miss for {} | {} | {}{}",
                key.storage_identity,
                key.file_path,
                key.etag,
                loaded ? "" : " (waited)");
            ProfileEvents::increment(ProfileEvents::PuffinFooterCacheMisses);
        }
        else
        {
            LOG_TRACE(log, "Puffin footer cache hit for {} | {} | {}", key.storage_identity, key.file_path, key.etag);
            ProfileEvents::increment(ProfileEvents::PuffinFooterCacheHits);
        }

        return cell->blobs;
    }

    template <typename LoadFunc>
    DataLakeObjectMetadata::ExcludedRowsPtr getOrSetDeletionVector(const PuffinFilesCacheKey & key, LoadFunc && load_fn)
    {
        /// True if this caller's load_fn ran. Used only for tracing; hit/miss metrics use the
        /// atomic CacheGetOrSetOutcome from getOrSetWithOutcome (not a follow-up contains()).
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
                    "Cached empty puffin deletion vector for {} | {} | {} at offset {} length {} for data file {}",
                    key.storage_identity,
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
                    "Loaded puffin deletion vector into cache for {} | {} | {} at offset {} length {} for data file {}",
                    key.storage_identity,
                    key.file_path,
                    key.etag,
                    key.content_offset,
                    key.content_size_in_bytes,
                    key.referenced_data_file);
            }
            return std::make_shared<PuffinFilesCacheCell>(std::move(excluded_rows));
        };

        auto [cell, outcome] = Base::getOrSetWithOutcome(key, load_fn_wrapper);
        const bool served_from_cache = outcome == CacheGetOrSetOutcome::Hit;
        if (!served_from_cache)
        {
            if (loaded && outcome == CacheGetOrSetOutcome::MissNotResident)
            {
                LOG_TRACE(
                    log,
                    "Puffin files cache miss (load discarded by concurrent clear) for {} | {} | {} at offset {} length {} for data file {}",
                    key.storage_identity,
                    key.file_path,
                    key.etag,
                    key.content_offset,
                    key.content_size_in_bytes,
                    key.referenced_data_file);
            }
            else if (!loaded && outcome == CacheGetOrSetOutcome::MissNotResident)
            {
                LOG_TRACE(
                    log,
                    "Puffin files cache miss (waited for load discarded by concurrent clear) for {} | {} | {} at offset {} length {} for data file {}",
                    key.storage_identity,
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
                    "Puffin files cache miss for {} | {} | {} at offset {} length {} for data file {}",
                    key.storage_identity,
                    key.file_path,
                    key.etag,
                    key.content_offset,
                    key.content_size_in_bytes,
                    key.referenced_data_file);
            }
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheMisses);
        }
        else if (cell->is_empty_deletion_vector)
        {
            LOG_TRACE(
                log,
                "Puffin files cache hit (empty deletion vector) for {} | {} | {} at offset {} length {} for data file {}",
                key.storage_identity,
                key.file_path,
                key.etag,
                key.content_offset,
                key.content_size_in_bytes,
                key.referenced_data_file);
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheHits);
        }
        else
        {
            LOG_TRACE(
                log,
                "Puffin files cache hit for {} | {} | {} at offset {} length {} for data file {}",
                key.storage_identity,
                key.file_path,
                key.etag,
                key.content_offset,
                key.content_size_in_bytes,
                key.referenced_data_file);
            ProfileEvents::increment(ProfileEvents::PuffinFilesCacheHits);
        }

        return cloneExcludedRows(*cell);
    }

private:
    static DataLakeObjectMetadata::ExcludedRowsPtr cloneExcludedRows(const PuffinFilesCacheCell & cell);

    LoggerPtr log;
    PuffinFooterCache footer_cache;

    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using PuffinFilesCachePtr = std::shared_ptr<PuffinFilesCache>;

}
