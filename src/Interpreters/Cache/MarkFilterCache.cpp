#include <Interpreters/Cache/MarkFilterCache.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{


std::vector<bool> MarkFilterCache::getByCondition(const MergeTreeDataPartPtr & data_part, const String & condition)
{
    std::lock_guard lock(mutex);

    auto table = data_part->storage.getStorageID();
    Key key {table.uuid, data_part->name, condition};

    auto entry = get(key);
    if (!entry)
        return {};

    return entry->filter;
}

void MarkFilterCache::update(const MergeTreeDataPartPtr & data_part, const String & condition, const MarkRanges & mark_ranges, bool exists)
{
    std::lock_guard lock(mutex);

    auto table = data_part->storage.getStorageID();
    Key key {table.uuid, data_part->name, condition};
    auto entry = getOrSet(key);
    auto & filter = entry->filter;

    size_t count = data_part->index_granularity.getMarksCount();
    if (filter.size() != count)
        filter.resize(count, true);

    for (const auto & mark_range : mark_ranges)
        std::fill(filter.begin() + mark_range.begin, filter.begin() + mark_range.end, exists);
}

MarkFilterCache::EntryPtr MarkFilterCache::get(const Key & key)
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

MarkFilterCache::EntryPtr MarkFilterCache::getOrSet(const Key & key)
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

// void MarkFilterCache::set(const Key & key, const EntryPtr & entry)
// {
//     TableMetadataPtr table_metadata;
//     if (const auto it = cache.find(key.table_id); it == cache.end())
//         table_metadata = it->second;
//     else
//     {
//         table_metadata = std::make_shared<TableMetadata>();
//         cache.insert({key.table_id, table_metadata});
//     }
//
//     table_metadata->setEntryAndUpdateQueue(key, entry, queue);
//
//     removeOverflow();
// }

void MarkFilterCache::removeOverflow()
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

bool MarkFilterCache::remove(const Key & key)
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

void MarkFilterCache::removeTable(const UUID & table_id)
{
    std::lock_guard lock(mutex);

    const auto it = cache.find(table_id);
    if (it == cache.end())
        return;

    for (const auto & [part_name, part_metadata] : *it->second)
    {
        for (const auto & [condition, entry] : *part_metadata)
        {
            queue.erase(entry->queue_iterator);
        }
    }

    cache.erase(it);
}

void MarkFilterCache::removePart(const UUID & table_id, const String & part_name)
{
    std::lock_guard lock(mutex);

    const auto it = cache.find(table_id);
    if (it == cache.end())
        return;

    auto & table_metadata = it->second;

    auto part_metadata_it = table_metadata->find(part_name);
    if (part_metadata_it == table_metadata->end())
        return;

    for (const auto & [condition, entry] : *part_metadata_it->second)
    {
        queue.erase(entry->queue_iterator);
    }

    table_metadata->erase(part_metadata_it);
    if (table_metadata->size() == 0)
        cache.erase(it);
}

// MarkFilterCache::TableMetadataPtr MarkFilterCache::getTableMetadata(const Key & key)
// {
//     // std::lock_guard lock(mutex);
//     if (const auto it = cache.find(key.table_id); it != cache.end())
//         return it->second;
//
//     return nullptr;
// }
//
// MarkFilterCache::TableMetadataPtr MarkFilterCache::getOrSetTableMetadata(const Key & key)
// {
//     // std::lock_guard lock(mutex);
//     if (const auto it = cache.find(key.table_id); it != cache.end())
//         return it->second;
//
//     auto table_metadata = std::make_shared<TableMetadata>();
//     cache.insert({key.table_id, table_metadata});
//     return table_metadata;
// }

MarkFilterCache::PartMetadataPtr MarkFilterCache::TableMetadata::getPartMetadata(const String & part_name)
{
    if (const auto it = find(part_name); it != end())
        return it->second;

    return nullptr;
}

// MarkFilterCache::PartMetadataPtr MarkFilterCache::TableMetadata::getOrSetPartMetadata(const Key & key)
// {
//     // std::lock_guard lock(mutex);
//     if (const auto it = find(key.part_name); it != end())
//         return it->second;
//
//     auto part_metadata = std::make_shared<PartMetadata>();
//     insert({key.part_name, part_metadata});
//     return part_metadata;
// }

MarkFilterCache::EntryPtr MarkFilterCache::TableMetadata::tryGetEntry(const Key & key)
{
    const auto it = find(key.part_name);
    if (it == end())
        return nullptr;

    return it->second->tryGetEntry(key);
}

std::tuple<bool, MarkFilterCache::EntryPtr> MarkFilterCache::TableMetadata::getOrSet(const Key & key)
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


// void MarkFilterCache::TableMetadata::setEntryAndUpdateQueue(const Key & key, const EntryPtr & entry, LRUQueue & queue)
// {
//     PartMetadataPtr part_metadata;
//     if (const auto it = find(key.part_name); it == end())
//         part_metadata = it->second;
//     else
//     {
//         part_metadata = std::make_shared<PartMetadata>();
//         insert({key.part_name, part_metadata});
//     }
//
//     part_metadata->setEntryAndUpdateQueue(key, entry, queue);
// }

bool MarkFilterCache::TableMetadata::remove(const Key & key)
{
    auto it = find(key.part_name);
    if (it == end())
        return false;

    auto part_metadata = it->second;
    if (part_metadata->remove(key))
    {
        if (part_metadata->size() == 0)
            erase(it);

        return true;
    }

    return false;
}

MarkFilterCache::EntryPtr MarkFilterCache::PartMetadata::tryGetEntry(const Key & key)
{
    if (const auto it = find(key.condition); it != end())
        return it->second;

    return nullptr;
}

std::tuple<bool, MarkFilterCache::EntryPtr> MarkFilterCache::PartMetadata::getOrSet(const Key & key)
{
    auto it = find(key.condition);
    if (it == end())
    {
        auto entry = insert({key.condition, std::make_shared<Entry>()}).first->second;
        return std::make_tuple(true, entry);
    }
    return std::make_tuple(false, it->second);
}


// void MarkFilterCache::PartMetadata::setEntryAndUpdateQueue(const Key & key, const EntryPtr & entry, LRUQueue & queue)
// {
//     // std::lock_guard lock(mutext);
//     auto [it, inserted] = insert({key.condition, entry});
//     EntryPtr & mapped = it->second;
//
//     if (inserted)
//     {
//         try
//         {
//             mapped->queue_iterator = queue.insert(queue.end(), key);
//         }
//         catch (...)
//         {
//             erase(it);
//             throw;
//         }
//     }
//     else
//     {
//         mapped = entry;
//         queue.splice(queue.end(), queue, mapped->queue_iterator);
//     }
// }



}
