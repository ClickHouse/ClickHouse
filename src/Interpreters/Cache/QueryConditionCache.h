#pragma once

#include <Common/CacheBase.h>
#include "Interpreters/Cache/QueryResultCache.h"
#include <Storages/MergeTree/MarkRange.h>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{
/// Key + entry represent a mark range result.
struct QueryConditionCacheKey
{
    const UUID table_id;
    const String part_name;
    const size_t condition_hash;

    bool operator==(const QueryConditionCacheKey & other) const;
};

struct QueryConditionCacheKeyHasher
{
    size_t operator()(const QueryConditionCacheKey & key) const;
};

/// False means none of the rows in the mark match the predicate. We can skip such marks.
/// True means at least one row in the mark matches the predicate. We need to read such marks.
using MatchingMarks = std::vector<bool>;
using MatchingMarksPtr = std::shared_ptr<MatchingMarks>;

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
private:
    struct QueryConditionCacheEntryWeight
    {
        size_t operator()(const MatchingMarks & entry) const;
    };

public:
    using Cache = CacheBase<QueryConditionCacheKey, MatchingMarks, QueryConditionCacheKeyHasher, QueryConditionCacheEntryWeight>;

    QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Add an entry to the cache. The passed marks represent ranges of the column with matches of the predicate.
    void write(const QueryConditionCacheKey & key, const MatchingMarks & matching_marks);

    /// Check the cache if it contains an entry for the given table + part id and predicate hash.
    MatchingMarksPtr read(const UUID & table_id, const String & part_name, size_t condition_hash);

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

class QueryConditionCacheWriter
{
public:
    QueryConditionCacheWriter(QueryConditionCachePtr query_condition_cache_, double query_condition_cache_zero_ratio_threshold_, size_t condition_hash_)
    : query_condition_cache(query_condition_cache_) 
    , query_condition_cache_zero_ratio_threshold(query_condition_cache_zero_ratio_threshold_)
    , condition_hash(condition_hash_)
    {}

    ~QueryConditionCacheWriter() { finalize(); }

    void buffer(const UUID & table_id, const String & part_name, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark);

private:
    void finalize();

    QueryConditionCachePtr query_condition_cache;
    double query_condition_cache_zero_ratio_threshold;
    size_t condition_hash;

    std::unordered_map<QueryConditionCacheKey, MatchingMarks, QueryConditionCacheKeyHasher> buffered;
    std::mutex mutex;

    LoggerPtr logger = getLogger("QueryConditionCache");
};

using QueryConditionCacheWriterPtr = std::shared_ptr<QueryConditionCacheWriter>;

}
