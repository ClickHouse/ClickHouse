#include <Interpreters/Cache/PartAggregationCache.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/SipHash.h>

#include <algorithm>


namespace DB
{

IASTHash PartAggregationCache::calculateQueryHash(
    const Names & keys,
    const AggregateDescriptions & aggregates,
    const ActionsDAG * filter_dag)
{
    SipHash hash;

    /// Hash keys and aggregates in their original order. Canonicalizing the order
    /// (sorting) would alias cache entries whose column layout differs positionally,
    /// e.g. `GROUP BY k1, k2` vs `GROUP BY k2, k1`. The cached block layout is fixed
    /// by `AggregatingStep` column order, so the hash must distinguish these cases.
    hash.update(keys.size());
    for (const auto & key : keys)
        hash.update(key);

    hash.update(aggregates.size());
    for (const auto & agg : aggregates)
    {
        hash.update(agg.column_name);
        hash.update(agg.function->getName());
        for (const auto & arg : agg.argument_names)
            hash.update(arg);
        for (const auto & param : agg.parameters)
            hash.update(param.dump());
    }

    if (filter_dag)
    {
        auto outputs = filter_dag->getOutputs();
        for (const auto * output : outputs)
            hash.update(output->result_name);
    }

    return getSipHash128AsPair(hash);
}


bool PartAggregationCache::Key::operator==(const Key & other) const
{
    return query_hash == other.query_hash && table_id == other.table_id && part_name == other.part_name;
}

size_t PartAggregationCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.query_hash.low64);
    hash.update(key.query_hash.high64);
    hash.update(key.table_id);
    hash.update(key.part_name);
    return hash.get64();
}

size_t PartAggregationCache::Entry::sizeInBytes() const
{
    return block.allocatedBytes();
}

PartAggregationCache::PartAggregationCache(size_t max_size_in_bytes_)
    : max_size_in_bytes(max_size_in_bytes_)
{
}

PartAggregationCache::EntryPtr PartAggregationCache::get(const Key & key) const
{
    std::lock_guard lock(mutex);

    auto it = cache.find(key);
    if (it == cache.end())
        return nullptr;

    lru_list.splice(lru_list.begin(), lru_list, it->second.lru_iterator);

    return it->second.entry;
}

void PartAggregationCache::set(const Key & key, Block block)
{
    auto new_entry = std::make_shared<Entry>(Entry{.block = std::move(block)});
    size_t entry_bytes = new_entry->sizeInBytes();

    std::lock_guard lock(mutex);

    if (entry_bytes > max_size_in_bytes)
        return;

    auto existing_it = cache.find(key);
    if (existing_it != cache.end())
        removeEntry(key);

    while (current_size_in_bytes + entry_bytes > max_size_in_bytes && !lru_list.empty())
        evictIfNeeded();

    lru_list.push_front(key);
    cache[key] = CacheEntry{.entry = std::move(new_entry), .lru_iterator = lru_list.begin()};
    current_size_in_bytes += entry_bytes;

    part_name_to_keys[key.part_name].push_back(key);
}

void PartAggregationCache::clear()
{
    std::lock_guard lock(mutex);
    cache.clear();
    lru_list.clear();
    part_name_to_keys.clear();
    current_size_in_bytes = 0;
}

void PartAggregationCache::invalidateByPartName(const String & part_name)
{
    std::lock_guard lock(mutex);

    auto it = part_name_to_keys.find(part_name);
    if (it == part_name_to_keys.end())
        return;

    auto keys_to_remove = std::move(it->second);
    part_name_to_keys.erase(it);

    for (const auto & key : keys_to_remove)
    {
        auto cache_it = cache.find(key);
        if (cache_it != cache.end())
        {
            current_size_in_bytes -= cache_it->second.entry->sizeInBytes();
            lru_list.erase(cache_it->second.lru_iterator);
            cache.erase(cache_it);
        }
    }
}

size_t PartAggregationCache::sizeInBytes() const
{
    std::lock_guard lock(mutex);
    return current_size_in_bytes;
}

size_t PartAggregationCache::entryCount() const
{
    std::lock_guard lock(mutex);
    return cache.size();
}

std::vector<PartAggregationCache::DumpEntry> PartAggregationCache::dump() const
{
    std::lock_guard lock(mutex);

    std::vector<DumpEntry> result;
    result.reserve(cache.size());

    for (const auto & [key, cache_entry] : cache)
    {
        result.push_back(DumpEntry{
            .key = key,
            .size_in_bytes = cache_entry.entry->sizeInBytes(),
            .rows = cache_entry.entry->block.rows(),
        });
    }

    return result;
}

void PartAggregationCache::updateConfiguration(size_t max_size_in_bytes_)
{
    std::lock_guard lock(mutex);
    max_size_in_bytes = max_size_in_bytes_;
    while (current_size_in_bytes > max_size_in_bytes && !lru_list.empty())
        evictIfNeeded();
}

void PartAggregationCache::evictIfNeeded()
{
    if (lru_list.empty())
        return;

    const Key & evict_key = lru_list.back();
    removeEntry(evict_key);
}

void PartAggregationCache::removeEntry(const Key & key)
{
    auto it = cache.find(key);
    if (it == cache.end())
        return;

    current_size_in_bytes -= it->second.entry->sizeInBytes();
    lru_list.erase(it->second.lru_iterator);
    cache.erase(it);

    auto idx_it = part_name_to_keys.find(key.part_name);
    if (idx_it != part_name_to_keys.end())
    {
        auto & keys = idx_it->second;
        keys.erase(std::remove(keys.begin(), keys.end(), key), keys.end());
        if (keys.empty())
            part_name_to_keys.erase(idx_it);
    }
}

}
