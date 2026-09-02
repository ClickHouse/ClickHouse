#include <Common/SipHash.h>
#include <Common/CurrentMetrics.h>
#include <Storages/MergeTree/PartStatisticsCache.h>

namespace CurrentMetrics
{
    extern const Metric PartStatisticsCacheBytes;
    extern const Metric PartStatisticsCacheCells;
}

namespace DB
{

size_t PartStatisticsWeightFunction::operator()(const ColumnsStatistics & statistics) const
{
    return PART_STATISTICS_CACHE_OVERHEAD + statistics.memoryUsageBytes();
}

template class CacheBase<UInt128, ColumnsStatistics, UInt128TrivialHash, PartStatisticsWeightFunction>;

PartStatisticsCache::PartStatisticsCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : Base(cache_policy, CurrentMetrics::PartStatisticsCacheBytes, CurrentMetrics::PartStatisticsCacheCells, max_size_in_bytes, 0, size_ratio)
{
}

UInt128 PartStatisticsCache::hash(const String & part_path, UInt128 content_checksum)
{
    SipHash hash;
    hash.update(part_path.data(), part_path.size() + 1);
    hash.update(content_checksum);
    return hash.get128();
}

}
