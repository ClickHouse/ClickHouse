#pragma once

#include <Common/CacheBase.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

/// An implementation of predicate caching a la https://doi.org/10.1145/3626246.3653395
///
/// Given the table + part IDs and a hash of a predicate as key, caches which marks definitely don't match the predicate and which marks may
/// match the predicate. This allows to skip the scan if the same predicate is evaluated on the same data again. Note that this doesn't work
/// the other way round: we can't tell if _all_ rows in the mark match the predicate.
///
/// Note: The cache may store more than the minimal number of matching marks. For example, assume a very selective predicate that matches
/// just a single row in a single mark. One would expect that the cache records just a single mark as potentially matching:
///     000000010000000000000000000
/// But it is equally correct for the cache to store this.
///     000001111111110000000000000
/// It is just less efficient for pruning (false positives).
class QueryConditionCache
{
public:
    /// False means none of the rows in the mark match the predicate. We can skip such marks.
    /// True means at least one row in the mark matches the predicate. We need to read such marks.
    using Entry = std::vector<bool>;
    using EntryPtr = std::shared_ptr<Entry>;

private:
    /// Key + entry represent a mark range result.
    struct Key
    {
        const UUID table_id;
        const String part_name;
        const UInt64 condition_hash;

        /// -- Additional members, conceptually not part of the key. Only included for pretty-printing
        ///    in system.query_condition_cache:
        const String condition;

        bool operator==(const Key & other) const;
    };

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

public:
    using Cache = CacheBase<Key, Entry, KeyHasher, EntryWeight>;

    QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    void write(const Key & key, const Entry & entry);

    /// Check the cache if it contains an entry for the given table + part id and predicate hash.
    EntryPtr read(const UUID & table_id, const String & part_name, UInt64 condition_hash);

    /// For debugging and system tables
    std::vector<QueryConditionCache::Cache::KeyMapped> dump() const;

    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    size_t maxSizeInBytes() const;

private:
    Cache cache;
    LoggerPtr logger = getLogger("QueryConditionCache");

    friend class QueryConditionCacheWriter;
    friend class StorageSystemQueryConditionCache;
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;


/// This class exists in the scope of a query or a sub-query.
/// AddRanges() creates/updates entries for the query condition cache (one entry per part), finalize() inserts them into the cache.
class QueryConditionCacheWriter
{
public:
    QueryConditionCacheWriter(
        QueryConditionCache & query_condition_cache_,
        size_t condition_hash_,
        const String & condition_,
        double selectivity_threshold_);

    ~QueryConditionCacheWriter();

    void addRanges(
        const UUID & table_id, const String & part_name,
        const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark);

private:
    void finalize();

    QueryConditionCache & query_condition_cache;
    const size_t condition_hash;
    const String condition;
    const double selectivity_threshold;

    std::unordered_map<QueryConditionCache::Key, QueryConditionCache::Entry, QueryConditionCache::KeyHasher> new_entries;
    std::mutex mutex;

    LoggerPtr logger = getLogger("QueryConditionCache");
};

using QueryConditionCacheWriterPtr = std::shared_ptr<QueryConditionCacheWriter>;

}
