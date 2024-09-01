#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Cache the mark filter corresponding to the query condition,
/// which helps to quickly filter out useless Marks and speed up the query when the index is not hit.

class QueryConditionCache
{
public:
    QueryConditionCache(size_t max_count_)
        : max_count(max_count_)
    {}

    using MarkFilter = std::vector<bool>;

    /// Read the filter and return empty if it does not exist.
    std::optional<MarkFilter> read(const MergeTreeDataPartPtr & data_part, const String & condition);

    /// Take out the mark filter corresponding to the query condition and set it to false on the corresponding mark.
    void write(const MergeTreeDataPartPtr & data_part, const String & condition, const MarkRanges & mark_ranges);

    void removeTable(const StorageID & table_id);

    void removeParts(const MergeTreeData::DataPartsVector & remove);

private:
    struct Key
    {
        UUID table_id;
        String part_name;
        String condition;
    };
    using KeyPtr = std::shared_ptr<Key>;

    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct Entry
    {
        MarkFilter filter;
        LRUQueueIterator queue_iterator;
    };
    using EntryPtr = std::shared_ptr<Entry>;

    struct PartMetadata : std::unordered_map<String, EntryPtr>
    {
        EntryPtr tryGetEntry(const Key & key);

        std::tuple<bool, EntryPtr> getOrSet(const Key & key);

        bool remove(const Key & key) { return erase(key.condition); }

    };
    using PartMetadataPtr = std::shared_ptr<PartMetadata>;

    struct TableMetadata : std::unordered_map<String, PartMetadataPtr>
    {
        PartMetadataPtr getPartMetadata(const String & part_name);

        EntryPtr tryGetEntry(const Key & key);

        std::tuple<bool, EntryPtr> getOrSet(const Key & key);

        bool remove(const Key & key);

        bool removePart(const String & part_name);

    };
    using TableMetadataPtr = std::shared_ptr<TableMetadata>;

    using Cache = std::unordered_map<UUID, TableMetadataPtr>;

    EntryPtr get(const Key & key);
    EntryPtr getOrSet(const Key & key);

    bool remove(const Key & key);

    void removeOverflow();

    size_t max_count;

    Cache cache;
    LRUQueue queue;
    std::mutex mutex;
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;

}

