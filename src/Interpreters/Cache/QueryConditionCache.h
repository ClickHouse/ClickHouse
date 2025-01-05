#pragma once

#include <Common/CacheBase.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

/// Cache the mark filter corresponding to the query condition,
/// which helps to quickly filter out useless Marks and speed up the query when the index is not hit.
class QueryConditionCache
{
public:
    /// 0 means none of the rows in the mark match the predicate. We can skip such marks.
    /// 1 means at least one row in the mark matches the predicate. We need to read such marks.
    using MatchingMarks = std::vector<bool>;

    QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Read the filter and return empty if it does not exist.
    std::optional<MatchingMarks> read(const UUID & table_id, const String & part_name, size_t condition_hash);

    /// Take out the mark filter corresponding to the query condition and set it to false on the corresponding mark.
    void write(const UUID & table_id, const String & part_name, size_t condition_hash, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark);

    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);

private:
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
        std::mutex mutex;

        explicit Entry(size_t mark_count);
    };

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    using Cache = CacheBase<Key, Entry, KeyHasher>;
    Cache cache;
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;

}
