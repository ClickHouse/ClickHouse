#include <Common/CurrentMetrics.h>
#include <Storages/MergeTree/SelectivityEstimatorCache.h>

namespace CurrentMetrics
{
    extern const Metric SelectivityEstimatorCacheBytes;
    extern const Metric SelectivityEstimatorCacheCells;
}

namespace DB
{

template class CacheBase<UInt128, ConditionSelectivityEstimator, UInt128TrivialHash, SelectivityEstimatorWeightFunction>;

SelectivityEstimatorCache::SelectivityEstimatorCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : Base(cache_policy, CurrentMetrics::SelectivityEstimatorCacheBytes, CurrentMetrics::SelectivityEstimatorCacheCells, max_size_in_bytes, 0, size_ratio)
{
}

}
