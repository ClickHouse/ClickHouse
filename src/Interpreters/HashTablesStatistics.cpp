#include <Interpreters/HashTablesStatistics.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

std::optional<HashTablesStatistics::Entry> HashTablesStatistics::getSizeHint(const Params & params)
{
    if (!params.isCollectionAndUseEnabled())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

    std::lock_guard lock(mutex);
    const auto cache = getHashTableStatsCache(params, lock);
    if (const auto hint = cache->get(params.key))
    {
        LOG_TRACE(
            getLogger("HashTablesStatistics"),
            "An entry for key={} found in cache: sum_of_sizes={}, median_size={}",
            params.key,
            hint->sum_of_sizes,
            hint->median_size);
        return *hint;
    }
    return std::nullopt;
}

/// Collection and use of the statistics should be enabled.
void HashTablesStatistics::update(size_t sum_of_sizes, size_t median_size, const Params & params)
{
    if (!params.isCollectionAndUseEnabled())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

    std::lock_guard lock(mutex);
    const auto cache = getHashTableStatsCache(params, lock);
    const auto hint = cache->get(params.key);
    // We'll maintain the maximum among all the observed values until another prediction is much lower (that should indicate some change)
    if (!hint || sum_of_sizes < hint->sum_of_sizes / 2 || hint->sum_of_sizes < sum_of_sizes || median_size < hint->median_size / 2
        || hint->median_size < median_size)
    {
        LOG_TRACE(
            getLogger("HashTablesStatistics"),
            "Statistics updated for key={}: new sum_of_sizes={}, median_size={}",
            params.key,
            sum_of_sizes,
            median_size);
        cache->set(params.key, std::make_shared<Entry>(Entry{.sum_of_sizes = sum_of_sizes, .median_size = median_size}));
    }
}

std::optional<HashTablesCacheStatistics> HashTablesStatistics::getCacheStats() const
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

HashTablesStatistics::CachePtr HashTablesStatistics::getHashTableStatsCache(const Params & params, const std::lock_guard<std::mutex> &)
{
    if (!hash_table_stats)
        hash_table_stats = std::make_shared<Cache>(params.max_entries_for_hash_table_stats * sizeof(Entry));
    return hash_table_stats;
}

HashTablesStatistics & getHashTablesStatistics()
{
    static HashTablesStatistics hash_tables_stats;
    return hash_tables_stats;
}

std::optional<HashTablesCacheStatistics> getHashTablesCacheStatistics()
{
    return getHashTablesStatistics().getCacheStats();
}

std::optional<HashTablesStatistics::Entry> getSizeHint(const DB::StatsCollectingParams & stats_collecting_params, size_t tables_cnt)
{
    if (stats_collecting_params.isCollectionAndUseEnabled())
    {
        if (auto hint = DB::getHashTablesStatistics().getSizeHint(stats_collecting_params))
        {
            const auto lower_limit = hint->sum_of_sizes / tables_cnt;
            const auto upper_limit = stats_collecting_params.max_size_to_preallocate / tables_cnt;
            if (hint->median_size > upper_limit)
            {
                /// Since we cannot afford to preallocate as much as needed, we would likely have to do at least one resize anyway.
                /// Though it still sounds better than N resizes, but in actuality we saw that one big resize (remember, HT-s grow exponentially)
                /// plus worse cache locality since we're dealing with big HT-s from the beginning yields worse performance.
                /// So let's just do nothing.
                LOG_TRACE(
                    getLogger("HashTablesStatistics"),
                    "No space were preallocated in hash tables because 'max_size_to_preallocate' has too small value: {}, "
                    "should be at least {}",
                    stats_collecting_params.max_size_to_preallocate,
                    hint->median_size * tables_cnt);
            }
            /// https://github.com/ClickHouse/ClickHouse/issues/44402#issuecomment-1359920703
            else if ((tables_cnt > 1 && hint->sum_of_sizes > 100'000) || hint->sum_of_sizes > 500'000)
            {
                return HashTablesStatistics::Entry{hint->sum_of_sizes, std::max(lower_limit, hint->median_size)};
            }
        }
    }
    return std::nullopt;
}
}
