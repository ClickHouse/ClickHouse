#pragma once

#include <Core/Block.h>
#include <Core/Names.h>
#include <Interpreters/AggregateDescription.h>
#include <Parsers/IASTHash.h>

#include <memory>
#include <mutex>
#include <list>
#include <unordered_map>


namespace DB
{

class ActionsDAG;

/// Cache for per-part intermediate aggregation states.
///
/// When a GROUP BY query is executed, the intermediate aggregation result (with `final = false`,
/// i.e. columns of type `AggregateFunction(...)`) for each MergeTree data part can be stored here.
/// On subsequent executions of the same query, cached states are reused for parts that still exist,
/// and only new or changed parts need to be aggregated from scratch. The cached per-part states are
/// then merged together with freshly computed states to produce the final result.
///
/// This is conceptually similar to aggregate projections, but created dynamically based on executed
/// queries rather than being defined statically in the table schema.
class PartAggregationCache
{
public:
    /// Identifies a cached aggregation result for a specific (query, part) pair.
    struct Key
    {
        /// Hash of the normalized query AST (GROUP BY keys, aggregate functions, WHERE clause).
        IASTHash query_hash;
        /// Name of the data part. Since parts are immutable, the name uniquely identifies content.
        /// If a part is merged or mutated, a new part with a different name is created.
        String part_name;

        bool operator==(const Key & other) const;
    };

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    /// A cached per-part aggregation result.
    struct Entry
    {
        /// Block containing columns with intermediate aggregate states (ColumnAggregateFunction).
        /// Produced by `Aggregator::convertToBlocks(data_variants, final=false)`.
        Block block;

        size_t sizeInBytes() const;
    };

    using EntryPtr = std::shared_ptr<const Entry>;

    /// Compute a stable hash for the aggregation query, used as part of the cache key.
    /// The hash captures GROUP BY keys, aggregate function signatures, and the filter expression.
    static IASTHash calculateQueryHash(
        const Names & keys,
        const AggregateDescriptions & aggregates,
        const ActionsDAG * filter_dag);

    explicit PartAggregationCache(size_t max_size_in_bytes_);

    /// Look up a cached aggregation result for the given (query, part) pair.
    /// Returns nullptr if not found.
    EntryPtr get(const Key & key) const;

    /// Store an aggregation result for the given (query, part) pair.
    void set(const Key & key, Block block);

    /// Remove all entries from the cache.
    void clear();

    /// Remove all entries associated with a specific part (e.g. after merge or mutation).
    void invalidateByPartName(const String & part_name);

    size_t sizeInBytes() const;
    size_t entryCount() const;

    /// Dump all cache entries for introspection (e.g. for the system table).
    struct DumpEntry
    {
        Key key;
        size_t size_in_bytes;
        size_t rows;
    };
    std::vector<DumpEntry> dump() const;

    void updateConfiguration(size_t max_size_in_bytes_);

private:
    /// LRU eviction: most recently used entries are at the front of the list.
    using LRUList = std::list<Key>;
    using LRUIterator = LRUList::iterator;

    struct CacheEntry
    {
        EntryPtr entry;
        LRUIterator lru_iterator;
    };

    mutable std::mutex mutex;
    mutable std::unordered_map<Key, CacheEntry, KeyHasher> cache;
    mutable LRUList lru_list;
    size_t max_size_in_bytes;
    size_t current_size_in_bytes = 0;

    /// Secondary index: part_name -> set of keys, for fast invalidation.
    std::unordered_map<String, std::vector<Key>> part_name_to_keys;

    void evictIfNeeded();
    void removeEntry(const Key & key);
};

using PartAggregationCachePtr = std::shared_ptr<PartAggregationCache>;

}
