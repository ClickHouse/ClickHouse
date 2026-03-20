#include <Functions/AIEmbed/EmbeddingCache.h>


namespace DB
{

EmbeddingCache::EmbeddingCache(size_t max_entries_, size_t max_memory_bytes_)
    : max_entries(max_entries_), max_memory_bytes(max_memory_bytes_)
{
}

std::optional<std::vector<Float32>> EmbeddingCache::get(UInt128 key)
{
    if (max_entries == 0)
    {
        ++stat_misses;
        return std::nullopt;
    }

    std::unique_lock lock(mutex);
    auto it = lru_map.find(key);
    if (it == lru_map.end())
    {
        ++stat_misses;
        return std::nullopt;
    }

    /// Move to front (most recently used)
    lru_list.splice(lru_list.begin(), lru_list, it->second);
    ++stat_hits;
    return it->second->embedding;
}

void EmbeddingCache::put(UInt128 key, std::vector<Float32> embedding)
{
    if (max_entries == 0)
        return;

    std::unique_lock lock(mutex);

    auto it = lru_map.find(key);
    if (it != lru_map.end())
    {
        /// Update existing entry, move to front
        current_memory_bytes -= entryMemory(it->second->embedding);
        it->second->embedding = std::move(embedding);
        current_memory_bytes += entryMemory(it->second->embedding);
        lru_list.splice(lru_list.begin(), lru_list, it->second);
        return;
    }

    size_t new_entry_mem = entryMemory(embedding);

    /// Insert new entry at front
    lru_list.push_front(Entry{key, std::move(embedding)});
    lru_map[key] = lru_list.begin();
    current_memory_bytes += new_entry_mem;

    evictIfNeeded();
}

void EmbeddingCache::drop()
{
    std::unique_lock lock(mutex);
    lru_list.clear();
    lru_map.clear();
    current_memory_bytes = 0;
}

EmbeddingCache::Stats EmbeddingCache::getStats() const
{
    std::shared_lock lock(mutex);
    return Stats{
        .hits = stat_hits.load(),
        .misses = stat_misses.load(),
        .evictions = stat_evictions,
        .entries = lru_map.size(),
        .memory_bytes = current_memory_bytes,
    };
}

EmbeddingCache & EmbeddingCache::instance()
{
    static EmbeddingCache cache(100000, 512 * 1024 * 1024);
    return cache;
}

void EmbeddingCache::updateLimits(size_t max_entries_, size_t max_memory_bytes_)
{
    std::unique_lock lock(mutex);
    max_entries = max_entries_;
    max_memory_bytes = max_memory_bytes_;
    evictIfNeeded();
}

void EmbeddingCache::evictIfNeeded()
{
    while (!lru_list.empty() && (lru_map.size() > max_entries || current_memory_bytes > max_memory_bytes))
    {
        auto & back = lru_list.back();
        current_memory_bytes -= entryMemory(back.embedding);
        lru_map.erase(back.key);
        lru_list.pop_back();
        ++stat_evictions;
    }
}

}
