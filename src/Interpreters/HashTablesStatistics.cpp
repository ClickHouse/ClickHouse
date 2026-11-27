#include <Interpreters/HashTablesStatistics.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <typename Entry>
std::optional<Entry> HashTablesStatistics<Entry>::getSizeHint(const Params & params)
{
    if (!params.isCollectionAndUseEnabled())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

    const auto cache = getHashTableStatsCache(params);
    if (const auto hint = cache->get(params.key))
    {
        LOG_TRACE(getLogger("HashTablesStatistics"), "An entry for key={} found in cache: {}", params.key, hint->dump());
        return *hint;
    }
    return std::nullopt;
}

/// Collection and use of the statistics should be enabled.
template <typename Entry>
void HashTablesStatistics<Entry>::update(const Entry & new_entry, const Params & params)
{
    if (!params.isCollectionAndUseEnabled())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

    const auto cache = getHashTableStatsCache(params);
    const auto hint = cache->get(params.key);
    // We'll maintain the maximum among all the observed values until another prediction is much lower (that should indicate some change)
    if (!hint || hint->shouldBeUpdated(new_entry))
    {
        LOG_TRACE(getLogger("HashTablesStatistics"), "Statistics updated for key={}: {}", params.key, new_entry.dump());
        cache->set(params.key, std::make_shared<Entry>(new_entry));
    }
}

template <typename Entry>
std::optional<HashTablesCacheStatistics> HashTablesStatistics<Entry>::getCacheStats() const
{
    std::lock_guard lock(mutex);
    if (hash_table_stats)
    {
        size_t hits = 0;
        size_t misses = 0;
        hash_table_stats->getStats(hits, misses);
        return DB::HashTablesCacheStatistics{.entries = hash_table_stats->count(), .hits = hits, .misses = misses};
    }
    return std::nullopt;
}

template <typename Entry>
HashTablesStatistics<Entry>::CachePtr HashTablesStatistics<Entry>::getHashTableStatsCache(const Params & params)
{
    std::lock_guard lock(mutex);
    if (!hash_table_stats)
        hash_table_stats = std::make_shared<Cache>(CurrentMetrics::end(), CurrentMetrics::end(), params.max_entries_for_hash_table_stats * sizeof(Entry));
    return hash_table_stats;
}

std::optional<HashTablesCacheStatistics> getHashTablesCacheStatistics()
{
    HashTablesCacheStatistics res{};
    if (auto aggr_stats = getHashTablesStatistics<AggregationEntry>().getCacheStats())
    {
        res.entries += aggr_stats->entries;
        res.hits += aggr_stats->hits;
        res.misses += aggr_stats->misses;
    }
    if (auto hash_join_stats = getHashTablesStatistics<HashJoinEntry>().getCacheStats())
    {
        res.entries += hash_join_stats->entries;
        res.hits += hash_join_stats->hits;
        res.misses += hash_join_stats->misses;
    }
    return res;
}

std::optional<AggregationEntry> getSizeHint(const DB::StatsCollectingParams & stats_collecting_params, size_t tables_cnt)
{
    if (stats_collecting_params.isCollectionAndUseEnabled())
    {
        if (auto hint = DB::getHashTablesStatistics<AggregationEntry>().getSizeHint(stats_collecting_params))
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
                return AggregationEntry{hint->sum_of_sizes, std::max(lower_limit, hint->median_size)};
            }
        }
    }
    return std::nullopt;
}

std::optional<HashJoinEntry> getSizeHint(const DB::StatsCollectingParams & stats_collecting_params)
{
    if (stats_collecting_params.isCollectionAndUseEnabled())
    {
        if (auto hint = DB::getHashTablesStatistics<HashJoinEntry>().getSizeHint(stats_collecting_params))
        {
            if (hint->ht_size > stats_collecting_params.max_size_to_preallocate)
            {
                LOG_TRACE(
                    getLogger("HashTablesStatistics"),
                    "No space were preallocated in hash tables because 'max_size_to_preallocate' has too small value: {}, should be at "
                    "least {}",
                    stats_collecting_params.max_size_to_preallocate,
                    hint->ht_size);
            }
            return hint;
        }
    }
    return std::nullopt;
}

template class HashTablesStatistics<AggregationEntry>;
template class HashTablesStatistics<HashJoinEntry>;
}
