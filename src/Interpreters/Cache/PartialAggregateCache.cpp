#include <Interpreters/Cache/PartialAggregateCache.h>

#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Columns/IColumn.h>

namespace CurrentMetrics
{
    extern const Metric PartialAggregateCacheBytes;
    extern const Metric PartialAggregateCacheEntries;
}

namespace DB
{

bool PartialAggregateCache::Key::operator==(const Key & other) const
{
    return query_hash == other.query_hash
        && part_name == other.part_name
        && part_mutation_version == other.part_mutation_version;
}

size_t PartialAggregateCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.query_hash.low64);
    hash.update(key.query_hash.high64);
    hash.update(key.part_name);
    hash.update(key.part_mutation_version);
    return hash.get64();
}

size_t PartialAggregateCache::EntryWeight::operator()(const Entry & entry) const
{
    size_t weight = 0;
    for (const auto & column : entry.partial_aggregate.getColumns())
        weight += column->allocatedBytes();
    return weight;
}

PartialAggregateCache::PartialAggregateCache(size_t max_size_in_bytes, size_t max_entries)
    : cache(
        "LRU",
        CurrentMetrics::PartialAggregateCacheBytes,
        CurrentMetrics::PartialAggregateCacheEntries,
        max_size_in_bytes,
        max_entries,
        /*size_ratio=*/0.5)
{
}

std::optional<Block> PartialAggregateCache::get(const Key & key)
{
    auto entry = cache.get(key);
    if (entry)
    {
        LOG_TRACE(logger, "Cache hit for part {}", key.part_name);
        return entry->partial_aggregate;
    }

    LOG_TRACE(logger, "Cache miss for part {}", key.part_name);
    return std::nullopt;
}

void PartialAggregateCache::put(const Key & key, Block partial_aggregate)
{
    auto entry = std::make_shared<Entry>();
    entry->partial_aggregate = std::move(partial_aggregate);
    entry->created_at = std::chrono::system_clock::now();

    cache.set(key, entry);
    LOG_TRACE(logger, "Cached partial aggregate for part {}", key.part_name);
}

void PartialAggregateCache::clear()
{
    cache.clear();
}

size_t PartialAggregateCache::sizeInBytes() const
{
    return cache.sizeInBytes();
}

size_t PartialAggregateCache::count() const
{
    return cache.count();
}

}

