#pragma once

#include <base/types.h>
#include <base/extended_types.h>
#include <Common/HashTable/Hash.h>

#include <Common/SharedMutex.h>

#include <atomic>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

/// Thread-safe LRU cache for embedding results, shared across queries.
/// Keyed by SipHash128(model_name + input_text) → vector<Float32>.
class EmbeddingCache
{
public:
    explicit EmbeddingCache(size_t max_entries_, size_t max_memory_bytes_);

    /// Returns cached embedding or std::nullopt.
    std::optional<std::vector<Float32>> get(UInt128 key);

    /// Insert embedding into cache. Evicts LRU entries if over capacity.
    void put(UInt128 key, std::vector<Float32> embedding);

    /// Clear all entries.
    void drop();

    struct Stats
    {
        size_t hits;
        size_t misses;
        size_t evictions;
        size_t entries;
        size_t memory_bytes;
    };

    Stats getStats() const;

    /// Singleton access. Initialized once with default settings; reconfigurable via settings at query time.
    static EmbeddingCache & instance();

    void updateLimits(size_t max_entries_, size_t max_memory_bytes_);

private:
    void evictIfNeeded();

    /// Approximate memory for one embedding entry: key (16 bytes) + vector data + list node overhead
    static size_t entryMemory(const std::vector<Float32> & embedding)
    {
        return sizeof(UInt128) + embedding.size() * sizeof(Float32) + 64; /// 64 bytes overhead for list/map nodes
    }

    struct Entry
    {
        UInt128 key;
        std::vector<Float32> embedding;
    };

    using LRUList = std::list<Entry>;
    using LRUMap = std::unordered_map<UInt128, LRUList::iterator, UInt128Hash>;

    mutable SharedMutex mutex;
    LRUList lru_list;
    LRUMap lru_map;
    size_t max_entries;
    size_t max_memory_bytes;
    size_t current_memory_bytes = 0;

    /// Stats
    mutable std::atomic<size_t> stat_hits{0};
    mutable std::atomic<size_t> stat_misses{0};
    size_t stat_evictions = 0;
};

}
