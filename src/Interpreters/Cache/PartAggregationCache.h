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
    struct Key
    {
        IASTHash query_hash;
        String table_id;
        String part_name;

        bool operator==(const Key & other) const;
    };

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct Entry
    {
        Block block;

        size_t sizeInBytes() const;
    };

    using EntryPtr = std::shared_ptr<const Entry>;

    static IASTHash calculateQueryHash(
        const Block & header,
        const Names & keys,
        const AggregateDescriptions & aggregates,
        const ActionsDAG * filter_dag);

    explicit PartAggregationCache(size_t max_size_in_bytes_);

    EntryPtr get(const Key & key) const;
    void set(const Key & key, Block block);
    void clear();
    void invalidateByPartName(const String & part_name);

    size_t sizeInBytes() const;
    size_t entryCount() const;

    struct DumpEntry
    {
        Key key;
        size_t size_in_bytes;
        size_t rows;
    };
    std::vector<DumpEntry> dump() const;

    void updateConfiguration(size_t max_size_in_bytes_);

private:
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

    std::unordered_map<String, std::vector<Key>> part_name_to_keys;

    void evictIfNeeded();
    void removeEntry(const Key & key);
};

using PartAggregationCachePtr = std::shared_ptr<PartAggregationCache>;

}
