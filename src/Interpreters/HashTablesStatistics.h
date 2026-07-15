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

    StatsCollectingParams & setKey(UInt64 key_)
    {
        key = key_;
        return *this;
    }

    UInt64 key = 0;
    const size_t max_entries_for_hash_table_stats = 0;
    const size_t max_size_to_preallocate = 0;
};

struct AggregationEntry
{
    bool shouldBeUpdated(const AggregationEntry & new_entry) const
    {
        return new_entry.sum_of_hash_table_sizes < sum_of_hash_table_sizes / 2
            || sum_of_hash_table_sizes < new_entry.sum_of_hash_table_sizes
            || new_entry.median_hash_table_size < median_hash_table_size / 2
            || median_hash_table_size < new_entry.median_hash_table_size
            || new_entry.merged_result_rows != merged_result_rows
            || new_entry.distinct_key_value_pairs != distinct_key_value_pairs;
    }

    std::string dump() const
    {
        return fmt::format(
            "sum_of_hash_table_sizes={}, median_hash_table_size={}, merged_result_rows={}, merged_hash_tables={}, "
            "distinct_key_value_pairs={}",
            sum_of_hash_table_sizes,
            median_hash_table_size,
            merged_result_rows,
            merged_hash_tables,
            distinct_key_value_pairs);
    }

    /// The entry counts of the per-stream partial hash tables, summed up. An upper bound on the
    /// number of group keys; used to decide whether to start two-level from the beginning.
    size_t sum_of_hash_table_sizes = 0;

    /// The median per-stream partial hash table size — roughly the amount to preallocate on each stream.
    size_t median_hash_table_size = 0;

    /// How many rows the merged aggregation produced, i.e. the number of group keys.
    size_t merged_result_rows = 0;

    /// How many per-stream partial hash tables the merge combined, empty ones included.
    size_t merged_hash_tables = 0;

    /// How many distinct (group key, argument value) pairs the aggregation saw — equivalently, the
    /// combined size of all the per-key distinct sets. Obtained by summing the produced `uniqExact`
    /// values over the result rows, so it is recorded only when `uniqExact` is the single aggregate
    /// and stays 0 otherwise.
    size_t distinct_key_value_pairs = 0;
};

struct HashJoinEntry
{
    bool shouldBeUpdated(const HashJoinEntry & new_entry) const { return new_entry.ht_size < ht_size / 2 || ht_size < new_entry.ht_size; }

    std::string dump() const { return fmt::format("ht_size={}", ht_size); }

    size_t ht_size; // the size of the shared hash table
    size_t source_rows; // the number of rows in the source table
};

/** Collects observed HashTable-s sizes to avoid redundant intermediate resizes.
  */
template <typename Entry>
class HashTablesStatistics
{
public:
    using Cache = DB::CacheBase<UInt64, Entry>;
    using CachePtr = std::shared_ptr<Cache>;
    using Params = StatsCollectingParams;

    /// Collection and use of the statistics should be enabled.
    std::optional<Entry> getSizeHint(const Params & params);

    /// Collection and use of the statistics should be enabled.
    void update(const Entry & new_entry, const Params & params);

    std::optional<DB::HashTablesCacheStatistics> getCacheStats() const;

private:
    CachePtr getHashTableStatsCache(const Params & params);

    mutable std::mutex mutex;
    CachePtr hash_table_stats TSA_GUARDED_BY(mutex);
};

template <typename Entry>
HashTablesStatistics<Entry> & getHashTablesStatistics()
{
    static HashTablesStatistics<Entry> hash_tables_stats;
    return hash_tables_stats;
}

std::optional<HashTablesCacheStatistics> getHashTablesCacheStatistics();

std::optional<AggregationEntry> getSizeHint(const DB::StatsCollectingParams & stats_collecting_params, size_t tables_cnt);
std::optional<HashJoinEntry> getSizeHint(const DB::StatsCollectingParams & stats_collecting_params);
}
