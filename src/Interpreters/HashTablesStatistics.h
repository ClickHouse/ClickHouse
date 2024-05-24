#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>
#include <Common/CacheBase.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>


namespace DB
{

struct HashTablesCacheStatistics
{
    size_t entries = 0;
    size_t hits = 0;
    size_t misses = 0;
};

inline size_t calculateCacheKey(const DB::ASTPtr & select_query)
{
    if (!select_query)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Query ptr cannot be null");

    const auto & select = select_query->as<DB::ASTSelectQuery &>();

    // It may happen in some corner cases like `select 1 as num group by num`.
    if (!select.tables())
        return 0;

    SipHash hash;
    hash.update(select.tables()->getTreeHash(/*ignore_aliases=*/true));
    if (const auto where = select.where())
        hash.update(where->getTreeHash(/*ignore_aliases=*/true));
    if (const auto group_by = select.groupBy())
        hash.update(group_by->getTreeHash(/*ignore_aliases=*/true));
    return hash.get64();
}

struct StatsCollectingParams
{
    StatsCollectingParams() = default;

    StatsCollectingParams(
        const ASTPtr & select_query_,
        bool collect_hash_table_stats_during_aggregation_,
        size_t max_entries_for_hash_table_stats_,
        size_t max_size_to_preallocate_for_aggregation_)
        : key(collect_hash_table_stats_during_aggregation_ ? calculateCacheKey(select_query_) : 0)
        , max_entries_for_hash_table_stats(max_entries_for_hash_table_stats_)
        , max_size_to_preallocate_for_aggregation(max_size_to_preallocate_for_aggregation_)
    {
    }

    bool isCollectionAndUseEnabled() const { return key != 0; }
    void disable() { key = 0; }

    UInt64 key = 0;
    const size_t max_entries_for_hash_table_stats = 0;
    const size_t max_size_to_preallocate_for_aggregation = 0;
};

/** Collects observed HashMap-s sizes to avoid redundant intermediate resizes.
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
    std::optional<Entry> getSizeHint(const Params & params)
    {
        if (!params.isCollectionAndUseEnabled())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

        std::lock_guard lock(mutex);
        const auto cache = getHashTableStatsCache(params, lock);
        if (const auto hint = cache->get(params.key))
        {
            LOG_TRACE(
                getLogger("Aggregator"),
                "An entry for key={} found in cache: sum_of_sizes={}, median_size={}",
                params.key,
                hint->sum_of_sizes,
                hint->median_size);
            return *hint;
        }
        return std::nullopt;
    }

    /// Collection and use of the statistics should be enabled.
    void update(size_t sum_of_sizes, size_t median_size, const Params & params)
    {
        if (!params.isCollectionAndUseEnabled())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

        std::lock_guard lock(mutex);
        const auto cache = getHashTableStatsCache(params, lock);
        const auto hint = cache->get(params.key);
        // We'll maintain the maximum among all the observed values until the next prediction turns out to be too wrong.
        if (!hint || sum_of_sizes < hint->sum_of_sizes / 2 || hint->sum_of_sizes < sum_of_sizes || median_size < hint->median_size / 2
            || hint->median_size < median_size)
        {
            LOG_TRACE(
                getLogger("Aggregator"),
                "Statistics updated for key={}: new sum_of_sizes={}, median_size={}",
                params.key,
                sum_of_sizes,
                median_size);
            cache->set(params.key, std::make_shared<Entry>(Entry{.sum_of_sizes = sum_of_sizes, .median_size = median_size}));
        }
    }

    std::optional<DB::HashTablesCacheStatistics> getCacheStats() const
    {
        std::lock_guard lock(mutex);
        if (hash_table_stats)
        {
            size_t hits = 0, misses = 0;
            hash_table_stats->getStats(hits, misses);
            return DB::HashTablesCacheStatistics{.entries = hash_table_stats->count(), .hits = hits, .misses = misses};
        }
        return std::nullopt;
    }

private:
    CachePtr getHashTableStatsCache(const Params & params, const std::lock_guard<std::mutex> &)
    {
        if (!hash_table_stats || hash_table_stats->maxSizeInBytes() != params.max_entries_for_hash_table_stats)
            hash_table_stats = std::make_shared<Cache>(params.max_entries_for_hash_table_stats);
        return hash_table_stats;
    }

    mutable std::mutex mutex;
    CachePtr hash_table_stats;
};

inline HashTablesStatistics & getHashTablesStatistics()
{
    static HashTablesStatistics hash_tables_stats;
    return hash_tables_stats;
}

inline std::optional<HashTablesCacheStatistics> getHashTablesCacheStatistics()
{
    return getHashTablesStatistics().getCacheStats();
}

}
