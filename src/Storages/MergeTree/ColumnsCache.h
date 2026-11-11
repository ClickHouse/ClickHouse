#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/IMemoryReleasableCache.h>
#include <Columns/IColumn.h>
#include <Core/Defines.h>
#include <Core/UUID.h>

namespace DB
{

class MergeTreeIndexGranularity;

/// Key of a cached deserialized column: one stripe of consecutive granules of one column of one
/// data part.
///
/// The stripes of a part are fixed - `ColumnsCacheStripes` cuts the marks of the part into
/// stripes of about `ColumnsCacheStripes::TARGET_ROWS` rows - so every read of the part finds the
/// same entries no matter how it cuts the part into ranges: the ranges of a read task depend on
/// the number of threads, on the primary key analysis, on the query condition cache and on
/// `PREWHERE` skipping rows, and they differ between queries and even between two runs of the
/// same query. An entry holds a contiguous run of granules of its stripe (the whole stripe after
/// a full scan, a part of it after a read that touched only some of its granules), so reads that
/// touch a few granules of a part still cache what they read, and adjacent runs written by
/// different reads are merged into one.
///
/// A stripe rather than a granule, because every entry costs a lookup and a copy per read, and
/// for a narrow column a granule is a few KiB: the fixed cost per entry then exceeds the cost of
/// decompressing the granule, and with many threads the lookups contend on the cache.
///
/// The column is identified by a hash of the table UUID, the part name, the column name and the
/// identity of the schema it was read with, computed once per reader, so that a lookup neither
/// copies strings nor hashes them. The table UUID keeps the entries across `RENAME TABLE` and
/// does not mix tables.
struct ColumnsCacheKey
{
    UInt128 column_identity = 0;
    size_t stripe = 0;

    bool operator==(const ColumnsCacheKey & other) const = default;
};

struct ColumnsCacheKeyHash
{
    size_t operator()(const ColumnsCacheKey & key) const
    {
        return intHash64(key.column_identity.items[UInt128::_impl::little(0)] ^ (key.stripe * 0x9E3779B97F4A7C15ULL));
    }
};

/// The identity of a column of a part for the cache, see `ColumnsCacheKey`.
///
/// `schema_identity` is a hash of the table's column list (names, types, defaults) taken from the
/// very metadata snapshot the reader uses, see `getColumnsCacheSchemaIdentity`. Cache data of an
/// earlier schema must not be served after an `ALTER` makes the same column name refer to a
/// different column, as in `RENAME a TO b, ADD COLUMN a`: the column list changes, so the entries
/// written before the `ALTER` are no longer reachable. Because the identity is a function of the
/// metadata snapshot itself and not of a separately maintained counter, it needs no coordination
/// with the moment the new metadata is published: a reader can never hold the new schema together
/// with the identity of the old one. It is stable across reads of an unchanged table, so repeated
/// reads find their entries.
UInt128 getColumnsCacheColumnIdentity(const UUID & table_uuid, const String & part_name, const String & column_name, UInt64 schema_identity);

/// How the marks of a part are cut into stripes: `stripe_marks` consecutive granules each, the
/// last stripe shorter. The same for every reader of the part, since it depends only on the
/// average size of a granule of the part.
struct ColumnsCacheStripes
{
    /// A stripe is about this many rows: one block of a read with the default `max_block_size`,
    /// so that a block served from the cache is one copy per column.
    static constexpr size_t TARGET_ROWS = 65536;
    static constexpr size_t MAX_STRIPE_MARKS = 256;

    size_t stripe_marks = 1;

    ColumnsCacheStripes() = default;
    explicit ColumnsCacheStripes(size_t avg_rows_per_mark);

    /// The stripes of a part, from the average size of its granules.
    static ColumnsCacheStripes forPart(const MergeTreeIndexGranularity & index_granularity);

    size_t stripeOf(size_t mark) const { return mark / stripe_marks; }
    size_t firstMark(size_t stripe) const { return stripe * stripe_marks; }
    size_t endMark(size_t stripe, size_t marks_in_part) const { return std::min((stripe + 1) * stripe_marks, marks_in_part); }
};

/// Cached deserialized column data: a contiguous run of granules of one stripe.
struct ColumnsCacheEntry
{
    ColumnPtr column;

    /// What the key was made of, for `system.columns_cache` and for the per-part index.
    UUID table_uuid = UUIDHelpers::Nil;
    String part_name;
    String column_name;
    UInt64 schema_identity = 0;

    /// The granules the entry holds, [first_mark, end_mark), and their rows.
    size_t first_mark = 0;
    size_t end_mark = 0;
    size_t row_begin = 0;
    size_t rows = 0;

    /// The key the entry is stored under. The eviction callback of the cache does not receive
    /// the key, and the cache has to know which entries of a part it holds, see `removePart`.
    ColumnsCacheKey key;

    /// Whether a read has found the entry since it was written, see `ColumnsCache::shouldAdmit`.
    mutable std::atomic<bool> used{false};

    bool coversMarks(size_t from_mark, size_t to_mark) const { return first_mark <= from_mark && to_mark <= end_mark; }
};

struct ColumnsCacheWeightFunction
{
    /// Overhead for key storage, hash map entry, shared pointers, the column object, etc.
    static constexpr size_t COLUMNS_CACHE_OVERHEAD = 512;

    size_t operator()(const ColumnsCacheEntry & entry) const
    {
        /// The memory the entry retains, not the logical size of the rows in it. `PODArray`
        /// rounds an allocation up to a power of two elements, so the capacity a column holds
        /// can exceed `byteSize` by up to two times; the copy is shrunk to its rows before it is
        /// admitted, but the allocator still rounds to its size classes.
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

/// Cache of deserialized columns for MergeTree tables.
/// Eliminates the need to read compressed data, decompress, and deserialize
/// for frequently accessed data parts and columns.
///
/// Sharded by key, like the userspace page cache, so that the threads of a query - and of the
/// queries running next to it - do not contend on one mutex: a read looks up one entry per
/// column per stripe (see `ColumnsCacheKey`), and these lookups go to different shards.
///
/// Every shard gets an equal share of the size, and an entry has to fit into the probationary
/// segment of its shard to be admitted, so a small cache is split into fewer shards: one per
/// `BYTES_PER_SHARD` of the configured size, up to `MAX_SHARDS`. The number of shards is fixed at
/// construction; a cache that is enlarged by a configuration reload keeps its shards.
class ColumnsCache : public IMemoryReleasableCache
{
public:
    using Key = ColumnsCacheKey;
    using Mapped = ColumnsCacheEntry;
    using MappedPtr = std::shared_ptr<Mapped>;

    static constexpr size_t MAX_SHARDS = 16;
    static constexpr size_t BYTES_PER_SHARD = 256_MiB;

    static size_t numberOfShards(size_t max_size_in_bytes) { return std::clamp<size_t>(max_size_in_bytes / BYTES_PER_SHARD, 1, MAX_SHARDS); }

    ColumnsCache(
        const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        size_t max_count,
        double size_ratio);

    ~ColumnsCache() override;

    /// The entries of the stripes [first_stripe, end_stripe) of a column: the entry of every
    /// stripe that is in the cache at its position, nullptr for the others. Each shard is locked
    /// once for all its keys.
    /// Does not update hit/miss profile events; the reader counts them per granule it reads.
    std::vector<MappedPtr> getMany(const UInt128 & column_identity, size_t first_stripe, size_t end_stripe);

    /// Insert the entries of several stripes. The key of every entry is `entry->key`.
    ///
    /// An entry that holds a part of a stripe is merged with the entry the cache holds for the
    /// stripe when the two overlap or touch, so that the runs of granules written by different
    /// reads add up to the whole stripe; an entry contained in the one the cache holds is
    /// dropped, and one that is disjoint from it replaces it only when it is larger.
    ///
    /// Returns the total weight of the entries that are in the cache afterwards and were not
    /// there before, for the per-query write budget.
    ///
    /// The expected generation is captured through getInvalidationGeneration
    /// when the reader starts. A mismatch means the table or the whole cache
    /// was invalidated after the read began, so the deferred write is dropped.
    size_t setMany(const std::vector<MappedPtr> & entries, UInt64 expected_table_generation);

    /// Whether a stripe that is not in the cache should be written to it. Nothing is written
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
    /// the start of a read and passes it back to `setMany`, so a deferred write issued
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

    /// Whether the cache holds any entry of the part. One lookup in the index, so that the cleanup
    /// of outdated parts can ask before it decides to clear the caches of a part.
    bool containsPart(const UUID & table_uuid, const String & part_name) const;

    /// Remove all cached entries for a specific table.
    /// Should be called on column metadata changes such as `RENAME COLUMN` that
    /// affect existing cache entries without rewriting parts. Cache keys identify
    /// columns by name, so a `RENAME a TO b; ADD COLUMN a` sequence could otherwise
    /// serve stale data for the freshly added `a`.
    void removeTable(const UUID & table_uuid);

    /// Drop every entry. Used by SYSTEM DROP COLUMNS CACHE.
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

    /// The size in effect, see `autoResize`; the configured size while the server has memory to spare.
    /// Also published as the `ColumnsCacheSizeLimit` metric.
    size_t maxSizeInBytes() const { return effective_max_size_in_bytes.load(std::memory_order_relaxed); }

    /// The size as configured by `columns_cache_size`: what `system.server_settings` reports, so
    /// that a configuration surface does not fluctuate with the memory pressure of the moment.
    size_t configuredMaxSizeInBytes() const { return configured_max_size_in_bytes.load(std::memory_order_relaxed); }
    size_t sizeInBytes() const;
    size_t count() const;

    /// Metadata for a cache entry, used by system.columns_cache.
    /// Does not hold a shared_ptr to column data, so it does not pin cached columns in memory.
    struct EntryMetadata
    {
        UUID table_uuid;
        String part_name;
        String column_name;
        size_t row_begin = 0;
        size_t rows = 0;
        size_t bytes = 0;
    };

    /// Get metadata for all cache entries for introspection (system.columns_cache table).
    /// Returns lightweight metadata without holding shared_ptrs to column data.
    std::vector<EntryMetadata> getAllEntriesMetadata();

    /// Test seam: `setMany` calls this after its entries have passed the generation check and have
    /// been recorded in the index, and before they are inserted into the shards. That is the window
    /// in which an invalidation makes the write stale without the first check being able to see it,
    /// and the recheck after the insertion is the only thing that takes the entries back out. Not
    /// set outside the tests, where checking one empty `std::function` per `setMany` is all it costs.
    std::function<void()> on_entries_staged_for_test;

    /// Test seam: `setMany` calls this right after its entries have been inserted into the shards
    /// and before the recheck that takes a stale write back out. That is the window in which a
    /// reader which started after the invalidation writes the same keys, and its entries are the
    /// ones the recheck has to leave alone, see `removeStaleEntries`.
    std::function<void()> on_entries_inserted_for_test;

private:
    using Base = CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>;

    class Shard : public Base
    {
    public:
        Shard(ColumnsCache & parent_, const String & cache_policy, CurrentMetrics::Metric size_in_bytes_metric, CurrentMetrics::Metric count_metric, size_t max_size_in_bytes, size_t max_count, double size_ratio)
            : Base(cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, max_count, size_ratio), parent(parent_)
        {
        }

        void onEntryRemoval(size_t weight_loss, const MappedPtr & mapped) override;

    private:
        ColumnsCache & parent;
    };

    std::vector<std::unique_ptr<Shard>> shards;

    size_t shardIndex(const Key & key) const { return ColumnsCacheKeyHash{}(key) % shards.size(); }
    Shard & shardOf(const Key & key) { return *shards[shardIndex(key)]; }

    struct PartIdentifier
    {
        UUID table_uuid;
        String part_name;

        bool operator==(const PartIdentifier & other) const = default;
    };

    struct PartIdentifierHash
    {
        size_t operator()(const PartIdentifier & id) const;
    };

    /// The stripes of a column of a part that are in the cache. The set is sparse on purpose:
    /// a bitmap would be sized by the highest stripe ever cached, so a single cached tail
    /// granule of a large part would keep a large allocation resident for as long as the part
    /// exists, outside the `columns_cache_size` bound. Here the memory is proportional to the
    /// entries the cache actually holds, and the buckets of a column and of a part are erased
    /// as soon as they become empty, see `forgetStripe`. What one resident stripe costs here - a
    /// hash set node - is an order of magnitude below the `COLUMNS_CACHE_OVERHEAD` every entry is
    /// already charged by `ColumnsCacheWeightFunction`, so the bound stays conservative.
    using CachedStripes = std::unordered_set<size_t>;
    using PartIndex = std::unordered_map<UInt128, CachedStripes, UInt128TrivialHash>;

    /// Which entries the cache holds, by part: what `removePart` and `removeTable` have to
    /// find quickly, and what the shards cannot answer without a scan of all their entries.
    /// A stripe is recorded by `setMany` before the entry is inserted into its shard and
    /// forgotten by `onEntryRemoval` when the entry is evicted. Guarded by `index_mutex`.
    ///
    /// Lock order: a shard's mutex is taken first and `index_mutex` second, because the
    /// eviction callback runs under the former and takes the latter. So nothing here ever
    /// calls into a shard while holding `index_mutex`.
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

    /// The size of the cache as configured, and the size in effect, which `autoResize` lowers
    /// while the server is short of memory. Every shard gets an equal share of it.
    std::atomic<size_t> configured_max_size_in_bytes;
    std::atomic<size_t> effective_max_size_in_bytes;

    /// The fraction of the server memory limit to keep free of the cache, see `autoResize`.
    std::atomic<double> free_memory_ratio{0.0};

    /// The peak of the memory used by everything but the cache over the last two history
    /// windows, so that `autoResize` does not grow the cache back at a brief dip of the
    /// memory usage only to evict it again a moment later. Guarded by `resize_mutex`, which
    /// also serializes the resizes themselves and the changes of the configured size.
    std::mutex resize_mutex;
    Int64 history_window_ms = 0;
    Int64 current_history_bucket = 0;
    size_t peak_memory_buckets[2] = {0, 0};

    /// Admission control, see `shouldAdmit`: the entries admitted and the entries evicted before
    /// any read found them, since the last adjustment, and the current admission probability as
    /// 2^-admission_log2.
    static constexpr size_t ADMISSION_WINDOW = 1024;
    static constexpr UInt64 MAX_ADMISSION_LOG2 = 6;
    std::atomic<size_t> recent_admitted{0};
    std::atomic<size_t> recent_evicted_unused{0};
    std::atomic<UInt64> admission_log2{0};

    void accountAdmitted(size_t admitted);

    void setShardsMaxSize(size_t total_max_size_in_bytes);

    /// Put a new size limit in effect: the shards, the value `maxSizeInBytes` returns and the
    /// `ColumnsCacheSizeLimit` metric always move together.
    void setEffectiveMaxSize(size_t max_size_in_bytes);

    /// Remove the given entries from the shards. Must be called without `index_mutex`.
    void removeFromShards(const std::vector<Key> & keys);

    /// Take back the entries of a `setMany` whose write turned out to be stale, and only those:
    /// the key can meanwhile belong to a reader that started after the invalidation.
    void removeStaleEntries(const std::vector<Key> & keys, const std::vector<MappedPtr> & entries);

    /// Forget one entry of a part in `part_index`, erasing the buckets of the column and of the
    /// part when they hold nothing any more, so that the index retains no memory for what the
    /// cache no longer holds. Must be called with `index_mutex` held.
    void forgetStripe(const PartIdentifier & part, const UInt128 & column_identity, size_t stripe);

    /// The entry to store for a stripe given what the cache holds for it: the new entry itself,
    /// the union of the two when they overlap or touch, or nothing when the new entry adds no
    /// granule. See `setMany`.
    static MappedPtr mergeEntries(const MappedPtr & existing, const MappedPtr & incoming);
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
