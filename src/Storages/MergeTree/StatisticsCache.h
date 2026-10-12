#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Storages/Statistics/Statistics.h>

namespace ProfileEvents
{
    extern const Event StatisticsCacheHits;
    extern const Event StatisticsCacheMisses;
}

namespace DB
{

/// Statistics of one column of one data part, as read from disk.
///
/// `stats` is nullptr when the part stores statistics for the column that cannot be used because they
/// were built for a different column type (a `MODIFY COLUMN` mutation is pending). That is cached too,
/// so every query does not re-read the part until the mutation replaces it.
struct StatisticsCacheCell
{
    ColumnStatisticsPtr stats;
    /// Uncompressed size of the serialized statistics, a close proxy for the in-memory size.
    size_t memory_bytes = 0;
};

struct StatisticsCacheWeightFunction
{
    /// The key in the hash map, the linked list nodes, the shared pointers, etc.
    static constexpr size_t STATISTICS_CACHE_OVERHEAD = 128;

    size_t operator()(const StatisticsCacheCell & cell) const { return STATISTICS_CACHE_OVERHEAD + cell.memory_bytes; }
};

extern template class CacheBase<UInt128, StatisticsCacheCell, UInt128TrivialHash, StatisticsCacheWeightFunction>;

/** Cache of column statistics of the data parts of MergeTree tables, one entry per part and column.
  *
  * A data part is immutable, so an entry never becomes stale: a new part is a miss, and a removed part
  * has its entries removed. A query merges the cached statistics of its parts on the fly, which is cheap,
  * so the merged statistics are never cached. The cached objects are shared between queries and must
  * not be modified: merge them into a fresh copy.
  */
class StatisticsCache : public CacheBase<UInt128, StatisticsCacheCell, UInt128TrivialHash, StatisticsCacheWeightFunction>
{
private:
    using Base = CacheBase<UInt128, StatisticsCacheCell, UInt128TrivialHash, StatisticsCacheWeightFunction>;

public:
    StatisticsCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// The key of the statistics of a column of the part stored at `part_path`.
    static UInt128 hash(const String & part_path, const String & column_name);

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::StatisticsCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::StatisticsCacheHits);

        return result.first;
    }
};

using StatisticsCachePtr = std::shared_ptr<StatisticsCache>;

}
