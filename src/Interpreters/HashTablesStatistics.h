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
    const size_t max_entries_for_hash_table_stats = 0; /// TODO: move to server settings
    const size_t max_size_to_preallocate = 0;
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

inline std::optional<HashTablesStatistics::Entry>
findSizeHint(const DB::StatsCollectingParams & stats_collecting_params, size_t max_threads)
{
    if (stats_collecting_params.isCollectionAndUseEnabled())
    {
        if (auto hint = DB::getHashTablesStatistics().getSizeHint(stats_collecting_params))
        {
            const auto lower_limit = hint->sum_of_sizes / max_threads;
            const auto upper_limit = stats_collecting_params.max_size_to_preallocate / max_threads;
            if (hint->median_size > upper_limit)
            {
                /// Since we cannot afford to preallocate as much as we want, we will likely need to do resize anyway.
                /// But we will also work with the big (i.e. not so cache friendly) HT from the beginning which may result in a slight slowdown.
                /// So let's just do nothing.
                LOG_TRACE(
                    getLogger("HashTablesStatistics"),
                    "No space were preallocated in hash tables because 'max_size_to_preallocate' has too small value: {}, "
                    "should be at least {}",
                    stats_collecting_params.max_size_to_preallocate,
                    hint->median_size * max_threads);
            }
            /// https://github.com/ClickHouse/ClickHouse/issues/44402#issuecomment-1359920703
            else if ((max_threads > 1 && hint->sum_of_sizes > 100'000) || hint->sum_of_sizes > 500'000)
            {
                const auto adjusted = std::max(lower_limit, hint->median_size);
                return HashTablesStatistics::Entry{hint->sum_of_sizes, adjusted};
            }
        }
    }
    return std::nullopt;
}
}
