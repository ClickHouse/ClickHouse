#pragma once

#include <Common/CacheBase.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

/// An implementation of predicate caching a la https://doi.org/10.1145/3626246.3653395
///
/// Given the table + part IDs and a hash of a predicate as key, caches which marks definitely don't
/// match the predicate and which marks may match the predicate. This allows to skip the scan if the
/// same predicate is evaluated on the same data again. Note that this doesn't work the other way
/// round: we can't tell if _all_ rows in the mark match the predicate.
class QueryConditionCache
{
public:
    /// False means none of the rows in the mark match the predicate. We can skip such marks.
    /// True means at least one row in the mark matches the predicate. We need to read such marks.
    using MatchingMarks = std::vector<bool>;

    QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Add an entry to the cache. The passed marks represent ranges of the column with matches of the predicate.
    void write(
        const UUID & table_id, const String & part_name, size_t predicate_hash,
        const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark);

    /// Check the cache if it contains an entry for the given table + part id and predicate hash.
    std::optional<MatchingMarks> read(const UUID & table_id, const String & part_name, size_t predicate_hash);

    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);

private:
    /// Key + entry represent a mark range result.
    struct Key
    {
        const UUID table_id;
        const String part_name;
        const size_t predicate_hash;

        bool operator==(const Key & other) const;
    };

    struct Entry
    {
        MatchingMarks matching_marks;

        explicit Entry(size_t mark_count);
    };

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct QueryConditionCacheEntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

    using Cache = CacheBase<Key, Entry, KeyHasher, QueryConditionCacheEntryWeight>;
    Cache cache;

    LoggerPtr logger = getLogger("QueryConditionCache");
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;

}
