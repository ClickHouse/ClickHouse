#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/Logger.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Common/SharedMutex.h>

namespace DB
{

/// An implementation of predicate caching a la https://doi.org/10.1145/3626246.3653395
///
/// Given the table, part name and a hash of a predicate as key, caches which marks definitely don't match the predicate and which marks may
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
    using MatchingMarks = std::vector<bool>;

private:
    /// A hash of the table id, part name and condition id.
    /// CityHash128 is enough to use for practical applications as the probability of collisions is very low.
    /// https://github.com/ClickHouse/ClickHouse/issues/9506
    using Key = UInt128;

    struct Entry
    {
#if defined(DEBUG_OR_SANITIZER_BUILD)
        /// Store extended information only in Debug builds.
        /// Having them in release builds is too costly.
        const UUID table_id;
        const String part_name;
        const UInt64 condition_hash = 42;
        const String condition;
#endif

        MatchingMarks matching_marks;
        SharedMutex mutex; /// (*)

        explicit Entry(size_t mark_count); /// (**)

#if defined(DEBUG_OR_SANITIZER_BUILD)
        Entry(size_t mark_count_, const UUID & table_id_, const String & part_name_, UInt64 condition_hash_, const String & condition_);
#endif

        /// (*) You might wonder why Entry has its own mutex considering that CacheBase locks internally already. The reason is that
        ///     ClickHouse scans ranges within the same part in parallel. The first scan creates and inserts a new Key + Entry into the cache,
        ///     the 2nd ... Nth scans find the existing Key and update its Entry for the new ranges. This can only be done safely in a
        ///     synchronized fashion.

        /// (**) About error handling: There could be an exception after the i-th scan and cache entries could (theoretically) be left in a
        ///     corrupt state. If we are not careful, future scans queries could then skip too many ranges. To prevent this, it is important to
        ///     initialize all marks of each entry as non-matching. In case of an exception, future scans will then not skip them.
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };


public:
    using Cache = CacheBase<Key, Entry, UInt128TrivialHash, EntryWeight>;

    /// Compute cache key from table UUID, part name and condition hash
    static Key makeKey(const UUID & table_id, const String & part_name, UInt64 condition_hash);

    QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Add an entry to the cache. The passed marks represent ranges of the column with matches of the predicate.
    void write(
        const UUID & table_id, const String & part_name, UInt64 condition_hash, const String & condition,
        const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark);

    /// Check the cache if it contains an entry for the given table + part id and predicate hash.
    std::optional<MatchingMarks> read(const UUID & table_id, const String & part_name, UInt64 condition_hash);

    /// For debugging and system tables
    std::vector<QueryConditionCache::Cache::KeyMapped> dump() const;

    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    size_t maxSizeInBytes() const;

private:
    Cache cache;
    LoggerPtr logger = getLogger("QueryConditionCache");

    friend class StorageSystemQueryConditionCache;
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;

}
