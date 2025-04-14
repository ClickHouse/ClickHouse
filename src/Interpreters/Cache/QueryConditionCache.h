#pragma once

#include <Common/CacheBase.h>
#include <Storages/MergeTree/MarkRange.h>
#include <shared_mutex>

namespace DB
{

/// An implementation of predicate caching a la https://doi.org/10.1145/3626246.3653395
///
/// Given the table + part IDs and a hash of a predicate as key, caches which marks definitely don't
/// match the predicate and which marks may match the predicate. This allows to skip the scan if the
/// same predicate is evaluated on the same data again. Note that this doesn't work the other way
/// round: we can't tell if _all_ rows in the mark match the predicate.
///
/// Note: The cache may store more than the minimal number of matching marks.
/// For example, assume a very selective predicate that matches just a single row in a single mark.
/// One would expect that the cache records just the single mark as potentially matching:
///     000000010000000000000000000
/// But it is equally correct for the cache to store this: (it is just less efficient for pruning)
///     000001111111110000000000000
class QueryConditionCache
{
public:
    /// False means none of the rows in the mark match the predicate. We can skip such marks.
    /// True means at least one row in the mark matches the predicate. We need to read such marks.
    using MatchingMarks = std::vector<bool>;

private:
    /// Key + entry represent a mark range result.
    struct Key
    {
        const UUID table_id;
        const String part_name;
        const size_t condition_hash;

        bool operator==(const Key & other) const;
    };

    struct Entry
    {
        MatchingMarks matching_marks;
        std::shared_mutex mutex; /// (*)

        explicit Entry(size_t mark_count); /// (**)

        /// (*) You might wonder why Entry has its own mutex considering that CacheBase locks internally already.
        ///     The reason is that ClickHouse scans ranges within the same part in parallel. The first scan creates
        ///     and inserts a new Key + Entry into the cache, the 2nd ... Nth scan find the existing Key and update
        ///     its Entry for the new ranges. This can only be done safely in a synchronized fashion.

        /// (**) About error handling: There could be an exception after the i-th scan and cache entries could
        ///     (theoretically) be left in a corrupt state. If we are not careful, future scans queries could then
        ///     skip too many ranges. To prevent this, it is important to initialize all marks of each entry as
        ///     non-matching. In case of an exception, future scans will then not skip them.

    };

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct QueryConditionCacheEntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

public:
    using Cache = CacheBase<Key, Entry, KeyHasher, QueryConditionCacheEntryWeight>;

    QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Add an entry to the cache. The passed marks represent ranges of the column with matches of the predicate.
    void write(
        const UUID & table_id, const String & part_name, size_t condition_hash,
        const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark);

    /// Check the cache if it contains an entry for the given table + part id and predicate hash.
    std::optional<MatchingMarks> read(const UUID & table_id, const String & part_name, size_t condition_hash);

    /// For debugging and system tables
    std::vector<QueryConditionCache::Cache::KeyMapped> dump() const;

    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    size_t maxSizeInBytes();

private:
    Cache cache;
    LoggerPtr logger = getLogger("QueryConditionCache");

    friend class StorageSystemQueryConditionCache;
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;

}
