#include <Interpreters/Cache/QueryConditionCache.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{


std::optional<QueryConditionCache::MarkFilter> QueryConditionCache::read(const MergeTreeDataPartPtr & data_part, const String & condition)
{
    if (!data_part)
        return std::nullopt;

    std::lock_guard lock(mutex);

    auto table = data_part->storage.getStorageID();
    Key key {table.uuid, data_part->name, condition};

    auto entry = get(key);
    if (!entry)
        return std::nullopt;

    return entry->filter;
}

void QueryConditionCache::write(const MergeTreeDataPartPtr & data_part, const String & condition, const MarkRanges & mark_ranges)
{
    if (!data_part)
        return;

    std::lock_guard lock(mutex);

    auto table_id = data_part->storage.getStorageID();
    Key key {table_id.uuid, data_part->name, condition};

    auto entry = getOrSet(key);
    auto & filter = entry->filter;

    /// By default, filters are all true.
    size_t count = data_part->index_granularity.getMarksCount();
    if (filter.size() != count)
        filter.resize(count, true);

    /// Set MarkRanges to false, so there is no need to read these marks again later.
    for (const auto & mark_range : mark_ranges)
        std::fill(filter.begin() + mark_range.begin, filter.begin() + mark_range.end, false);
}


void QueryConditionCache::removeTable(const StorageID & table_id)
{
    std::lock_guard lock(mutex);

    const auto it = cache.find(table_id.uuid);
    if (it == cache.end())
        return;

    for (const auto & [part_name, part_metadata] : *it->second)
    {
        for (const auto & [condition, entry] : *part_metadata)
            queue.erase(entry->queue_iterator);
    }

    cache.erase(it);
}

void QueryConditionCache::removeParts(const MergeTreeData::DataPartsVector & remove)
{
    if (remove.empty())
        return;

    std::lock_guard lock(mutex);
    auto table_id = remove.front()->storage.getStorageID();

    const auto it = cache.find(table_id.uuid);
    if (it == cache.end())
        return;

    auto & table_metadata = it->second;

    for (const auto & part : remove)
    {
        auto part_metadata_it = table_metadata->find(part->name);
        if (part_metadata_it == table_metadata->end())
            return;

        for (const auto & [condition, entry] : *part_metadata_it->second)
            queue.erase(entry->queue_iterator);

        table_metadata->erase(part_metadata_it);
    }

    if (table_metadata->empty())
        cache.erase(it);
}

QueryConditionCache::EntryPtr QueryConditionCache::get(const Key & key)
{
    const auto it = cache.find(key.table_id);
    if (it == cache.end())
        return nullptr;

    auto table_metadata = it->second;
    auto entry = table_metadata->tryGetEntry(key);
    if (!entry)
        return nullptr;

    /// Move the key to the end of the queue. The iterator remains valid.
    queue.splice(queue.end(), queue, entry->queue_iterator);
    return entry;
}

QueryConditionCache::EntryPtr QueryConditionCache::getOrSet(const Key & key)
{
    TableMetadataPtr table_metadata;
    if (const auto it = cache.find(key.table_id); it != cache.end())
        table_metadata = it->second;
    else
    {
        table_metadata = std::make_shared<TableMetadata>();
        cache.insert({key.table_id, table_metadata});
    }

    auto [created, entry] = table_metadata->getOrSet(key);
    if (created)
    {
        try
        {
            entry->queue_iterator = queue.insert(queue.end(), key);
            removeOverflow();
        }
        catch (...)
        {
            remove(key);
            throw;
        }
    }
    else
        queue.splice(queue.end(), queue, entry->queue_iterator);

    return entry;
}

void QueryConditionCache::removeOverflow()
{
    size_t queue_size = queue.size();
    while ((max_count != 0 && queue_size > max_count) && queue_size > 0)
    {
        if (const auto & key = queue.front(); !remove(key))
            std::terminate(); // Queue became inconsistent

        queue.pop_front();
        --queue_size;
    }
}

bool QueryConditionCache::remove(const Key & key)
{
    const auto it = cache.find(key.table_id);
    if (it == cache.end())
        return false;

    auto table_metadata = it->second;
    if (table_metadata->remove(key))
    {
        if (table_metadata->size() == 0)
            cache.erase(it);

        return true;
    }

    return false;
}

QueryConditionCache::PartMetadataPtr QueryConditionCache::TableMetadata::getPartMetadata(const String & part_name)
{
    if (const auto it = find(part_name); it != end())
        return it->second;

    return nullptr;
}

QueryConditionCache::EntryPtr QueryConditionCache::TableMetadata::tryGetEntry(const Key & key)
{
    const auto it = find(key.part_name);
    if (it == end())
        return nullptr;

    return it->second->tryGetEntry(key);
}

std::tuple<bool, QueryConditionCache::EntryPtr> QueryConditionCache::TableMetadata::getOrSet(const Key & key)
{
    PartMetadataPtr part_metadata;
    if (const auto it = find(key.part_name); it != end())
        part_metadata = it->second;
    else
    {
        part_metadata = std::make_shared<PartMetadata>();
        insert({key.part_name, part_metadata});
    }

    return part_metadata->getOrSet(key);
}

bool QueryConditionCache::TableMetadata::remove(const Key & key)
{
    auto it = find(key.part_name);
    if (it == end())
        return false;

    auto part_metadata = it->second;
    if (part_metadata->remove(key))
    {
        if (part_metadata->empty())
            erase(it);

        return true;
    }

    return false;
}

QueryConditionCache::EntryPtr QueryConditionCache::PartMetadata::tryGetEntry(const Key & key)
{
    if (const auto it = find(key.condition); it != end())
        return it->second;

    return nullptr;
}

std::tuple<bool, QueryConditionCache::EntryPtr> QueryConditionCache::PartMetadata::getOrSet(const Key & key)
{
    auto it = find(key.condition);
    if (it == end())
    {
        auto entry = insert({key.condition, std::make_shared<Entry>()}).first->second;
        return std::make_tuple(true, entry);
    }
    return std::make_tuple(false, it->second);
}

}
