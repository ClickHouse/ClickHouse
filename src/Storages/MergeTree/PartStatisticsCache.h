#pragma once
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/HashTable/Hash.h>
#include <Storages/Statistics/Statistics.h>

namespace ProfileEvents
{
    extern const Event PartStatisticsCacheHits;
    extern const Event PartStatisticsCacheMisses;
}

namespace DB
{

/// Estimate of number of bytes in cache for the statistics of one part.
struct PartStatisticsWeightFunction
{
    /// We spent additional bytes on key in hashmap, linked lists, shared pointers, etc ...
    static constexpr size_t PART_STATISTICS_CACHE_OVERHEAD = 128;

    size_t operator()(const ColumnsStatistics & statistics) const;
};

extern template class CacheBase<UInt128, ColumnsStatistics, UInt128TrivialHash, PartStatisticsWeightFunction>;

/** Cache of deserialized column statistics of data parts of MergeTree tables.
  * Statistics of a part are immutable, so entries are keyed by the part location plus the
  * checksum of its contents and invalidated when the part is removed.
  */
class PartStatisticsCache : public CacheBase<UInt128, ColumnsStatistics, UInt128TrivialHash, PartStatisticsWeightFunction>
{
private:
    using Base = CacheBase<UInt128, ColumnsStatistics, UInt128TrivialHash, PartStatisticsWeightFunction>;

public:
    /// The cache has a fixed budget instead of a server setting: statistics are small compared
    /// to the data they describe (tens of KiB per part), so a fixed bound suffices and the
    /// feature needs no configuration.
    static constexpr auto DEFAULT_POLICY = "SLRU";
    static constexpr size_t DEFAULT_MAX_SIZE_BYTES = 256 * 1024 * 1024;
    static constexpr double DEFAULT_SIZE_RATIO = 0.5;

    PartStatisticsCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Calculate key from the part path and the total checksum of the part's files.
    static UInt128 hash(const String & part_path, UInt128 content_checksum);

    MappedPtr get(const Key & key)
    {
        auto result = Base::get(key);
        ProfileEvents::increment(result ? ProfileEvents::PartStatisticsCacheHits : ProfileEvents::PartStatisticsCacheMisses);
        return result;
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::PartStatisticsCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::PartStatisticsCacheHits);

        return result.first;
    }
};

using PartStatisticsCachePtr = std::shared_ptr<PartStatisticsCache>;

}
