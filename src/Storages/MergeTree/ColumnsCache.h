#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include <Common/CacheBase.h>
#include <Common/IMemoryReleasableCache.h>
#include <Common/SipHash.h>
#include <Columns/IColumn.h>
#include <Core/UUID.h>

namespace DB
{

/// Key of a cached deserialized column: the rows of one granule of one column of one data part.
///
/// An entry covers exactly one granule (the rows between two adjacent marks), so the entries
/// of a part never overlap, and every read of the part finds the same entries no matter how
/// it cuts the part into ranges: the ranges of a read task depend on the number of threads,
/// on the primary key analysis, on the query condition cache and on `PREWHERE` skipping rows,
/// and they differ between queries and even between two runs of the same query. Entries
/// bound to those ranges would only ever be found again by a read with the very same ranges,
/// while the entries of the other reads of the same data would displace them.
///
/// Uses the table UUID so that `RENAME TABLE` keeps the entries and does not mix tables.
struct ColumnsCacheKey
{
    UUID table_uuid;
    String part_name;
    String column_name;
    /// The granule: the index of the mark the rows of the entry start at.
    size_t mark = 0;
    /// Identity of the schema the column was read with: a hash of the table's column list
    /// (names, types, defaults) taken from the very metadata snapshot the reader uses, see
    /// `getColumnsCacheSchemaIdentity`. Cache data of an earlier schema must not be served
    /// after an `ALTER` makes the same column name refer to a different column, as in
    /// `RENAME a TO b, ADD COLUMN a`: the column list changes, so the entries written before
    /// the `ALTER` are no longer reachable. Because the identity is a function of the
    /// metadata snapshot itself and not of a separately maintained counter, it needs no
    /// coordination with the moment the new metadata is published: a reader can never hold
    /// the new schema together with the identity of the old one. It is stable across reads
    /// of an unchanged table, so repeated reads find their entries.
    UInt64 schema_identity = 0;

    bool operator==(const ColumnsCacheKey & other) const = default;
};

struct ColumnsCacheKeyHash
{
    size_t operator()(const ColumnsCacheKey & key) const
    {
        SipHash hash;
        hash.update(key.table_uuid);
        hash.update(key.part_name);
        hash.update(key.column_name);
        hash.update(key.mark);
        hash.update(key.schema_identity);
        return hash.get64();
    }
};

/// Cached deserialized column data of one granule.
struct ColumnsCacheEntry
{
    ColumnPtr column;
    /// The first row of the granule in the part, and the number of rows in it.
    size_t row_begin = 0;
    size_t rows = 0;
    /// The key the entry is stored under. The eviction callback of the cache does not receive
    /// the key, and the cache has to know which granules of a part it holds, see `removePart`.
    ColumnsCacheKey key;
    /// Whether a read has found the entry since it was written, see `ColumnsCache::shouldAdmit`.
    mutable std::atomic<bool> used{false};
};

struct ColumnsCacheWeightFunction
{
    /// Overhead for key storage, hash map entry, shared pointers, the column object, etc.
    static constexpr size_t COLUMNS_CACHE_OVERHEAD = 512;

    size_t operator()(const ColumnsCacheEntry & entry) const
    {
        /// The memory the entry retains, not the logical size of the rows in it. `PODArray`
        /// rounds an allocation up to a power of two elements, so the capacity a column holds
        /// can exceed `byteSize` by up to two times; the accumulated copy is shrunk to its rows
        /// before it is admitted, but the allocator still rounds to its size classes.
        /// `columns_cache_size` is documented as a bound on the memory the cache keeps, so the
        /// bound has to be enforced - and reported by `system.columns_cache`,
        /// `CurrentMetrics::ColumnsCacheBytes` and `ProfileEvents::ColumnsCacheEvictedBytes` -
        /// on the memory that is actually held. `allocatedBytes` descends into the nested
        /// columns of `Array`, `Tuple`, `Nullable` and `Map`, so composite shapes are covered
        /// too, and unlike `byteSize` it also counts the dictionary of a `LowCardinality`
        /// column when that dictionary is shared. The entry keeps the dictionary alive - once
        /// the part is gone the cache can be its only holder - so it has to be charged; several
        /// entries of the same column do share one dictionary object, and each of them is
        /// charged for it, which makes the accounting conservative (the cache holds less than
        /// its limit) rather than unbounded.
        return entry.column->allocatedBytes() + COLUMNS_CACHE_OVERHEAD;
    }
};

extern template class CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>;

/// Cache of deserialized columns for MergeTree tables.
/// Eliminates the need to read compressed data, decompress, and deserialize
/// for frequently accessed data parts and columns.
///
/// The entries are looked up by exact key, one per granule (see `ColumnsCacheKey`), and the
/// reader asks for all the granules of a column of a range at once, so a range that is only
/// partly cached is served from the cache for the granules that are there and read from disk
/// for the others.
class ColumnsCache : public CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>, public IMemoryReleasableCache
{
private:
    using Base = CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>;

    struct PartIdentifier
    {
        UUID table_uuid;
        String part_name;

        bool operator==(const PartIdentifier & other) const = default;
    };

    struct PartIdentifierHash
    {
        size_t operator()(const PartIdentifier & id) const
        {
            SipHash hash;
            hash.update(id.table_uuid);
            hash.update(id.part_name);
            return hash.get64();
        }
    };

    struct ColumnIdentifier
    {
        String column_name;
        UInt64 schema_identity = 0;

        bool operator==(const ColumnIdentifier & other) const = default;
    };

    struct ColumnIdentifierHash
    {
        size_t operator()(const ColumnIdentifier & id) const
        {
            SipHash hash;
            hash.update(id.column_name);
            hash.update(id.schema_identity);
            return hash.get64();
        }
    };

    /// The granules of a column of a part that are in the cache: a bit per mark.
    using CachedMarks = std::vector<bool>;
    using PartIndex = std::unordered_map<ColumnIdentifier, CachedMarks, ColumnIdentifierHash>;

    /// Which entries the cache holds, by part: what `removePart` and `removeTable` have to
    /// find quickly, and what the base cache cannot answer without a scan of all its entries.
    /// A bit is set by `set` before the entry is inserted into the base cache and reset by
    /// `onEntryRemoval` when the entry is evicted. Guarded by `index_mutex`.
    ///
    /// Lock order: the base cache's mutex is taken first and `index_mutex` second, because the
    /// eviction callback runs under the former and takes the latter. So nothing here ever
    /// calls into the base cache while holding `index_mutex`.
    std::unordered_map<PartIdentifier, PartIndex, PartIdentifierHash> part_index;
    mutable std::mutex index_mutex;

    /// Per-table invalidation stamp, advanced by removeTable. See
    /// getInvalidationGeneration. Guarded by index_mutex.
    std::unordered_map<UUID, UInt64> table_generations;

    /// Cache-wide invalidation stamp, advanced by clearAll (`SYSTEM DROP COLUMNS
    /// CACHE`). It participates in the token returned by
    /// getInvalidationGeneration, so a drop also rejects deferred writes from
    /// readers that started before it. Guarded by index_mutex.
    UInt64 global_generation = 0;

    /// Source of invalidation stamps: every invalidation event takes the next value, so two
    /// different events never produce the same stamp. Guarded by index_mutex.
    UInt64 last_generation = 0;

    UInt64 nextGeneration() { return ++last_generation; }

    /// The invalidation token a reader captures for a table: the later of the cache-wide and
    /// the per-table stamp. Stamps come from a single monotonic source, so the token changes
    /// on every invalidation of the table and on every drop of the whole cache, and a token
    /// captured before an invalidation can never compare equal to the current one. This also
    /// lets clearAll forget the per-table stamps: the new cache-wide stamp is greater than
    /// every stamp handed out before it.
    /// Must be called with index_mutex held.
    UInt64 currentGeneration(const UUID & table_uuid) const
    {
        auto it = table_generations.find(table_uuid);
        return std::max(global_generation, it == table_generations.end() ? UInt64(0) : it->second);
    }

    /// The size of the cache as configured, and the size in effect (also `Base::maxSizeInBytes`,
    /// mirrored here to be read without the lock), which `autoResize` lowers while the server is
    /// short of memory.
    std::atomic<size_t> configured_max_size_in_bytes;
    std::atomic<size_t> effective_max_size_in_bytes;

    /// The fraction of the server memory limit to keep free of the cache, see `autoResize`.
    std::atomic<double> free_memory_ratio{0.0};

    /// The peak of the memory used by everything but the cache over the last two history
    /// windows, so that `autoResize` does not grow the cache back at a brief dip of the
    /// memory usage only to evict it again a moment later. Guarded by `resize_mutex`, which
    /// also serializes the resizes themselves.
    std::mutex resize_mutex;
    Int64 history_window_ms = 0;
    Int64 current_history_bucket = 0;
    size_t peak_memory_buckets[2] = {0, 0};

    /// Remove the given entries from the base cache. Must be called without `index_mutex`.
    void removeFromBase(const PartIdentifier & part_id, const PartIndex & index);

    /// Admission control, see `shouldAdmit`: the entries admitted and the entries evicted before
    /// any read found them, since the last adjustment, and the current admission probability as
    /// 2^-admission_log2.
    static constexpr size_t ADMISSION_WINDOW = 4096;
    static constexpr UInt64 MAX_ADMISSION_LOG2 = 6;
    std::atomic<size_t> recent_admitted{0};
    std::atomic<size_t> recent_evicted_unused{0};
    std::atomic<UInt64> admission_log2{0};

    void accountAdmitted(size_t admitted);

    void onEntryRemoval(size_t weight_loss, const MappedPtr & mapped) override;

public:
    ColumnsCache(
        const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        size_t max_count,
        double size_ratio);

    /// The entries of the granules [first_mark, end_mark) of a column: the entry of every
    /// granule that is in the cache at its position, nullptr for the others. One lookup under
    /// the lock for the whole range, so that a read of many granules does not contend on the
    /// cache for each of them.
    /// Does not update hit/miss profile events; the reader counts them per granule it reads.
    std::vector<MappedPtr> getMany(
        const UUID & table_uuid,
        const String & part_name,
        const String & column_name,
        UInt64 schema_identity,
        size_t first_mark,
        size_t end_mark);

    /// Insert the column of one granule into the cache. The key is `mapped->key`.
    /// Returns true if the entry is in the cache afterwards, false if the write was dropped:
    /// it was rejected as stale (see below) or the entry could not stay resident, e.g. its
    /// weight exceeds the size limit. Callers use the return value to avoid charging the
    /// per-query write budget for writes that never landed in the cache.
    ///
    /// The expected generation is captured through getInvalidationGeneration
    /// when the reader starts. A mismatch means the table or the whole cache
    /// was invalidated after the read began, so the deferred write is dropped.
    bool set(const MappedPtr & mapped, UInt64 expected_table_generation);

    /// Insert the entries of several granules at once - one pass under the locks for all of them,
    /// so that a read of many small granules does not contend on the cache for each of them.
    /// Returns the total weight of the entries that are in the cache afterwards, for the per-query
    /// write budget. The generation check is the same as in `set`, for all entries together.
    size_t setMany(const std::vector<MappedPtr> & entries, UInt64 expected_table_generation);

    /// Whether a granule that is not in the cache should be written to it. Nothing is written
    /// while the cache is shrunk to nothing by `autoResize`: the copy would be rejected anyway.
    ///
    /// A cache smaller than the working set of the queries is worse than no cache: with the
    /// entries of one pass evicted before the next pass reaches them, every read pays for
    /// copying its rows into the cache and none is served from it. The protected segment of
    /// the SLRU policy keeps what was found at least once, but the probationary segment churns.
    /// So the entries evicted before any read found them are counted against the entries
    /// admitted: while most of the admitted entries go unused, the admission probability is
    /// halved (down to 1/64), and while most of them are used, or nothing is evicted, it is
    /// doubled back. A cache that holds its working set is not affected: nothing is evicted.
    bool shouldAdmit();

    /// Current invalidation token for a table. Advances each time removeTable is
    /// called (a metadata change that can remap column names) and each time
    /// clearAll is called (`SYSTEM DROP COLUMNS CACHE`). A reader captures this at
    /// the start of a read and passes it back to `set`, so a deferred write issued
    /// by a reader that started before the invalidation cannot repopulate the
    /// cache with stale data, and cannot resurrect entries an explicit drop
    /// removed.
    UInt64 getInvalidationGeneration(const UUID & table_uuid);

    /// Remove all cached entries for a specific data part.
    /// Called when the part leaves the table for good: from the destructor of the part, and for
    /// outdated parts that nobody else holds any more (`isSharedPtrUnique`). A reader holds the
    /// part through its `data_part_info_for_read` for as long as its deferred write is pending,
    /// so no reader can still write entries for the part after this call, and unlike removeTable
    /// this needs no invalidation stamp: nothing per part is retained once the entries are gone.
    /// Part names are not reused within a table, so a later part cannot find these entries either.
    void removePart(const UUID & table_uuid, const String & part_name);

    /// Remove all cached entries for a specific table.
    /// Should be called on column metadata changes such as `RENAME COLUMN` that
    /// affect existing cache entries without rewriting parts. Cache keys identify
    /// columns by name, so a `RENAME a TO b; ADD COLUMN a` sequence could otherwise
    /// serve stale data for the freshly added `a`.
    void removeTable(const UUID & table_uuid);

    /// Clear both the base cache and the part index.
    /// Used by SYSTEM DROP COLUMNS CACHE.
    /// Advances the cache-wide invalidation generation first, so the drop is
    /// sticky: a reader that started before it cannot write its deferred entries
    /// back into the cache afterwards.
    void clearAll();

    /// Set the size of the cache from the configuration. Takes effect at once: the entries
    /// beyond the new size are evicted.
    void setConfiguredMaxSizeInBytes(size_t max_size_in_bytes);

    /// How `autoResize` behaves: the fraction of the memory limit that is kept free of the
    /// cache, and the length of the window the memory usage of the rest of the server is
    /// averaged (as a peak) over.
    void setAutoResizeSettings(double free_memory_ratio_, Int64 history_window_ms_);

    /// Give memory back to the queries when the server is short of it.
    ///
    /// The cache is bounded by `columns_cache_size`, but the bound counts against the same
    /// `max_server_memory_usage` as the queries do: on a server whose queries use most of its
    /// memory, a cache of a tenth of it pushes them over the limit, and they fail where they
    /// succeeded with the cache off. So the size in effect is lowered to what fits next to the
    /// peak memory usage of everything else - like the userspace page cache does - and raised
    /// again towards the configured size once that usage subsides:
    ///
    ///     target = min(configured size, memory_limit * (1 - free_memory_ratio) - peak usage excluding the cache)
    ///
    /// Called periodically by `MemoryWorker`, and by `MemoryTracker` when an allocation is about
    /// to exceed the limit, before it resorts to stopping a query. Returns true if the memory
    /// usage fits the limit after the resize.
    bool autoResize(Int64 memory_usage, size_t memory_limit) override;

    /// Metadata for a cache entry, used by system.columns_cache.
    /// Does not hold a shared_ptr to column data, so it does not pin cached columns in memory.
    struct EntryMetadata
    {
        Key key;
        size_t row_begin = 0;
        size_t rows = 0;
        size_t bytes = 0;
    };

    /// Get metadata for all cache entries for introspection (system.columns_cache table).
    /// Returns lightweight metadata without holding shared_ptrs to column data.
    std::vector<EntryMetadata> getAllEntriesMetadata();
};

using ColumnsCachePtr = std::shared_ptr<ColumnsCache>;

/// The size of the columns cache when `columns_cache_size` is not set in the server configuration:
/// `columns_cache_size_to_ram_ratio` of the memory available to the server, so that the cache scales
/// with the machine - a fixed size that suits a small server holds none of the working sets of the
/// heavier queries on a large one, and a cache that cannot hold what a query reads is pure overhead:
/// the query copies its data into the cache only to evict it again. Falls back to the built-in
/// default when the amount of memory is unknown.
size_t getDefaultColumnsCacheSize(size_t physical_server_memory, double size_to_ram_ratio);

/// Per-query shared accounting for columns cache writes.
/// One instance is created per query (in Context::makeQueryContext) and shared
/// by all of that query's read pools, so the documented per-query budgets
/// (`columns_cache_max_bytes_to_write_to_cache` and
/// `columns_cache_max_estimated_bytes_to_write_to_cache`) apply to the
/// query as a whole rather than to each `MergeTreeReadPoolBase` independently.
/// Without this, a query with several MergeTree read pipelines (for example a
/// `JOIN`, `UNION`, or subqueries) would let each pool write up to the full cap,
/// so total writes could exceed the configured budget by a multiple.
struct ColumnsCacheWriteBudget
{
    /// Running total of bytes actually written to the cache by this query.
    std::atomic<size_t> bytes_written{0};

    /// Running total of uncompressed bytes this query's read pools estimate they
    /// will read, accumulated part by part as the pools are constructed.
    std::atomic<size_t> estimated_bytes{0};

    /// Latches to true once `estimated_bytes` exceeds the estimate budget, after
    /// which every later pool of the query disables cache writes.
    std::atomic<bool> writes_disabled{false};
};

using ColumnsCacheWriteBudgetPtr = std::shared_ptr<ColumnsCacheWriteBudget>;

}
