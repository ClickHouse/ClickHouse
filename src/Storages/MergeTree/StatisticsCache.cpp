#include <Storages/MergeTree/StatisticsCache.h>

#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>

namespace CurrentMetrics
{
    extern const Metric StatisticsCacheBytes;
    extern const Metric StatisticsCacheCells;
}

namespace DB
{

template class CacheBase<UInt128, StatisticsCacheCell, UInt128TrivialHash, StatisticsCacheWeightFunction>;

StatisticsCache::StatisticsCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : Base(cache_policy, CurrentMetrics::StatisticsCacheBytes, CurrentMetrics::StatisticsCacheCells, max_size_in_bytes, 0, size_ratio)
{
}

UInt128 StatisticsCache::hash(const String & part_path, const String & column_name)
{
    SipHash hash;
    hash.update(part_path.data(), part_path.size() + 1);
    hash.update(column_name.data(), column_name.size() + 1);
    return hash.get128();
}

}
