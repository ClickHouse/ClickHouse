#pragma once
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/HashTable/Hash.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

namespace ProfileEvents
{
    extern const Event SelectivityEstimatorCacheHits;
    extern const Event SelectivityEstimatorCacheMisses;
}

namespace DB
{

/// Estimate of number of bytes in cache for one selectivity estimator.
struct SelectivityEstimatorWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t SELECTIVITY_ESTIMATOR_CACHE_OVERHEAD = 128;

    size_t operator()(const ConditionSelectivityEstimator & estimator) const
    {
        return SELECTIVITY_ESTIMATOR_CACHE_OVERHEAD + estimator.memoryUsageBytes();
    }
};

extern template class CacheBase<UInt128, ConditionSelectivityEstimator, UInt128TrivialHash, SelectivityEstimatorWeightFunction>;

/** Cache of selectivity estimators built from the statistics of a set of data parts.
  * Entries are keyed by (table, ordered part-name set, requested column set); parts are
  * immutable, so an equal key implies an equal estimator, and entries for part sets that
  * disappeared (merges, drops) age out by eviction.
  */
class SelectivityEstimatorCache : public CacheBase<UInt128, ConditionSelectivityEstimator, UInt128TrivialHash, SelectivityEstimatorWeightFunction>
{
private:
    using Base = CacheBase<UInt128, ConditionSelectivityEstimator, UInt128TrivialHash, SelectivityEstimatorWeightFunction>;

public:
    /// Fixed budget instead of a server setting, like `PartStatisticsCache`.
    static constexpr auto DEFAULT_POLICY = "SLRU";
    static constexpr size_t DEFAULT_MAX_SIZE_BYTES = 128 * 1024 * 1024;
    static constexpr double DEFAULT_SIZE_RATIO = 0.5;

    SelectivityEstimatorCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    MappedPtr get(const Key & key)
    {
        auto result = Base::get(key);
        if (result)
            ProfileEvents::increment(ProfileEvents::SelectivityEstimatorCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::SelectivityEstimatorCacheMisses);

        return result;
    }
};

using SelectivityEstimatorCachePtr = std::shared_ptr<SelectivityEstimatorCache>;

}
