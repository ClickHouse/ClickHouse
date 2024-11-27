#pragma once

#include <Common/CacheBase.h>

namespace DB
{

struct HashTablesCacheStatistics
{
    size_t entries = 0;
    size_t hits = 0;
    size_t misses = 0;
};

struct StatsCollectingParams
{
    StatsCollectingParams() = default;

    StatsCollectingParams(UInt64 key_, bool enable_, size_t max_entries_for_hash_table_stats_, size_t max_size_to_preallocate_)
        : key(enable_ ? key_ : 0)
        , max_entries_for_hash_table_stats(max_entries_for_hash_table_stats_)
        , max_size_to_preallocate(max_size_to_preallocate_)
    {
    }

    bool isCollectionAndUseEnabled() const { return key != 0; }
    void disable() { key = 0; }

    UInt64 key = 0;
    const size_t max_entries_for_hash_table_stats = 0;
    const size_t max_size_to_preallocate = 0;
};

/** Collects observed HashTable-s sizes to avoid redundant intermediate resizes.
  */
class HashTablesStatistics
{
public:
    struct Entry
    {
        size_t sum_of_sizes; // used to determine if it's better to convert aggregation to two-level from the beginning
        size_t median_size; // roughly the size we're going to preallocate on each thread
    };

    using Cache = DB::CacheBase<UInt64, Entry>;
    using CachePtr = std::shared_ptr<Cache>;
    using Params = StatsCollectingParams;

    /// Collection and use of the statistics should be enabled.
    std::optional<Entry> getSizeHint(const Params & params);

    /// Collection and use of the statistics should be enabled.
    void update(size_t sum_of_sizes, size_t median_size, const Params & params);

    std::optional<DB::HashTablesCacheStatistics> getCacheStats() const;

private:
    CachePtr getHashTableStatsCache(const Params & params, const std::lock_guard<std::mutex> &);

    mutable std::mutex mutex;
    CachePtr hash_table_stats;
};

HashTablesStatistics & getHashTablesStatistics();

std::optional<HashTablesCacheStatistics> getHashTablesCacheStatistics();

std::optional<HashTablesStatistics::Entry> getSizeHint(const DB::StatsCollectingParams & stats_collecting_params, size_t tables_cnt);
}
