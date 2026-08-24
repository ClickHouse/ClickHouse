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
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace ProfileEvents
{
extern const Event PuffinFilesCacheHits;
extern const Event PuffinFilesCacheMisses;
extern const Event PuffinFilesCacheWeightLost;
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

    /// Approximate bytes for key strings + fixed key fields (used in entry weight).
    UInt64 approximateMemoryBytes() const;
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

    PuffinFilesCacheCell(DataLakeObjectMetadata::ExcludedRowsPtr excluded_rows_, UInt64 key_memory_bytes_);

    static UInt64 calculateMemorySize(
        bool is_empty_deletion_vector_,
        const DataLakeObjectMetadata::ExcludedRowsPtr & excluded_rows_,
        UInt64 key_memory_bytes_);

    /// Lower-bound resident weight before the DV payload is known (key + cell object + overhead).
    /// Used to skip the in-memory cache path when `puffin_files_cache_size` cannot hold even one entry.
    static UInt64 estimateMinimumMemorySize(UInt64 key_memory_bytes_)
    {
        return calculateMemorySize(/*is_empty_deletion_vector_=*/true, nullptr, key_memory_bytes_);
    }

private:
    /// Hash-map node + LRU list node + shared_ptr control block underestimates are absorbed here.
    static constexpr size_t SIZE_IN_MEMORY_OVERHEAD = 256;
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

/// Cache for parsed content loaded from Puffin files (deletion vectors today, indexes later).
/// Also memoizes parsed footers so coalesced multi-DV Puffins parse the footer once per file.
///
/// Footer memo and DV LRU share one byte budget and one entry-count budget (`puffin_files_cache_size`
/// / max entries). Memo weight is charged into `CurrentMetrics::PuffinFilesCacheBytes` /
/// `PuffinFilesCacheFiles` so resident usage stays observable and cannot reach ~2× the configured max.
class PuffinFilesCache : public CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>
{
public:
    using Base = CacheBase<PuffinFilesCacheKey, PuffinFilesCacheCell, PuffinFilesCacheKeyHash, PuffinFilesCacheWeightFunction>;
    using FooterBlobsPtr = std::shared_ptr<const std::vector<PuffinBlob>>;

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

    /// Clears deletion-vector entries and footer memo.
    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    void setMaxCount(size_t max_count);

    /// Test/observability helpers for the footer memo.
    size_t footerMemoEntries() const;
    UInt64 footerMemoBytes() const;

    /// Small memo (not a weighted LRU): shares the configured byte/count limits with the DV cache.
    /// Concurrent misses on the same key each run `load_fn` (no stampede / waiter token);
    /// only coalesced sequential slice loads share one parse. On insert, entries are dropped
    /// one-by-one until the new entry fits beside current DV weight — not a full memo clear.
    template <typename LoadFunc>
    FooterBlobsPtr getOrSetFooter(const PuffinFooterCacheKey & key, LoadFunc && load_fn)
    {
        {
            std::lock_guard lock(footer_mutex);
            if (shared_max_bytes != 0)
            {
                if (auto it = footer_memo.find(key); it != footer_memo.end())
                    return it->second.blobs;
            }
        }

        auto blobs = load_fn();

        /// Snapshot DV occupancy outside `footer_mutex` to avoid Base↔memo lock-order inversion
        /// with `clear()` / `setMax*` (those take Base first, then memo).
        const size_t dv_bytes = Base::sizeInBytes();
        const size_t dv_count = Base::count();

        std::lock_guard lock(footer_mutex);
        if (shared_max_bytes == 0)
            return blobs;

        if (auto it = footer_memo.find(key); it != footer_memo.end())
            return it->second.blobs;

        const UInt64 entry_bytes = approximateFooterEntryBytes(key, blobs);
        /// Must fit beside current DV weight under the single shared byte budget.
        if (entry_bytes > shared_max_bytes || dv_bytes > shared_max_bytes - entry_bytes)
            return blobs;

        while (needsFooterEvictionForInsertUnlocked(dv_bytes, dv_count, entry_bytes))
        {
            if (footer_memo.empty())
                break;
            eraseFooterMemoVictimUnlocked();
        }

        if (needsFooterEvictionForInsertUnlocked(dv_bytes, dv_count, entry_bytes))
            return blobs;

        auto [it, inserted] = footer_memo.emplace(key, FooterMemoEntry{blobs, entry_bytes});
        if (inserted)
            accountFooterMemoInsertUnlocked(entry_bytes);
        return it->second.blobs;
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
            return std::make_shared<PuffinFilesCacheCell>(std::move(excluded_rows), key.approximateMemoryBytes());
        };

        auto [cell, outcome] = Base::getOrSetWithOutcome(key, load_fn_wrapper);
        /// DV weight may have grown; reclaim footer memo so DV + memo stay within one budget.
        trimFooterMemoToSharedBudget();

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
    struct FooterMemoEntry
    {
        FooterBlobsPtr blobs;
        UInt64 memory_bytes = 0;
    };

    static DataLakeObjectMetadata::ExcludedRowsPtr cloneExcludedRows(const PuffinFilesCacheCell & cell);
    static UInt64 approximateFooterEntryBytes(const PuffinFooterCacheKey & key, const FooterBlobsPtr & blobs);

    void clearFooterMemoUnlocked();
    void accountFooterMemoInsertUnlocked(UInt64 entry_bytes);
    void eraseFooterMemoVictimUnlocked();
    bool needsFooterEvictionForInsertUnlocked(size_t dv_bytes, size_t dv_count, UInt64 entry_bytes) const;
    void trimFooterMemoToSharedBudget();

    LoggerPtr log;
    mutable std::mutex footer_mutex;
    std::unordered_map<PuffinFooterCacheKey, FooterMemoEntry, PuffinFooterCacheKeyHash> footer_memo;
    /// Configured shared limits (same values passed to Base). Copied here so memo insert/trim can
    /// decide without calling back into Base while holding `footer_mutex`.
    size_t shared_max_count = 0;
    size_t shared_max_bytes = 0;
    UInt64 footer_memo_bytes = 0;

    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override;
};

using PuffinFilesCachePtr = std::shared_ptr<PuffinFilesCache>;

}
