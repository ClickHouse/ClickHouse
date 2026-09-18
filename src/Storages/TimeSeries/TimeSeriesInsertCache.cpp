#include <Storages/TimeSeries/TimeSeriesInsertCache.h>

#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
extern const Metric TimeSeriesInsertCacheBytes;
extern const Metric TimeSeriesInsertCacheEntries;
}

namespace DB
{

TimeSeriesInsertCache::TimeSeriesInsertCache(size_t max_size_in_bytes)
    : Base(
        "LRU",
        CurrentMetrics::TimeSeriesInsertCacheBytes,
        CurrentMetrics::TimeSeriesInsertCacheEntries,
        max_size_in_bytes,
        /*max_count=*/ 0,
        /*size_ratio=*/ 0)
{
}

bool TimeSeriesInsertCache::contains(UInt128 key_hash, UInt128 value_hash)
{
    auto cached = get(key_hash);
    return cached && *cached == value_hash;
}

void TimeSeriesInsertCache::insert(const std::vector<Entry> & entries)
{
    for (const auto & entry : entries)
        set(entry.key_hash, std::make_shared<UInt128>(entry.value_hash));
}

}
