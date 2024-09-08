#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

using MarkFilter = std::vector<bool>;

class IMergeTreeDataPart;

/// Cache the mark filter corresponding to the query condition,
/// which helps to quickly filter out useless Marks and speed up the query when the index is not hit.
class QueryConditionCache
{
public:
    QueryConditionCache(size_t max_count_)
        : cache(std::numeric_limits<size_t>::max(), max_count_)
    {}

    /// Read the filter and return empty if it does not exist.
    std::optional<MarkFilter> read(const std::shared_ptr<const IMergeTreeDataPart> & data_part, const String & condition);

    /// Take out the mark filter corresponding to the query condition and set it to false on the corresponding mark.
    void write(const MergeTreeDataPartPtr & data_part, const String & condition, const MarkRanges & mark_ranges);

    void clear() { cache.clear(); }

    void updateConfiguration(size_t max_entries) { cache.setMaxCount(max_entries); }

private:
    struct Key
    {
        const UUID table_id;
        const String part_name;
        const String condition;

        bool operator==(const Key & other) const { return table_id == other.table_id && part_name == other.part_name && condition == other.condition; }
    };

    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct Entry
    {
        /// If the mark at the corresponding position is false, there is no need to read.
        MarkFilter mark_filter;
        std::mutex mutex;

        /// The default filters are all set to true.
        Entry(size_t count)
            : mark_filter(count, true)
        {}
    };

    using Cache = CacheBase<Key, Entry, KeyHasher>;
    Cache cache;
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;

}

