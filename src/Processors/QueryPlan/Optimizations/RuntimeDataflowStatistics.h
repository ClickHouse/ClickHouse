#pragma once

#include <Common/CacheBase.h>

#include <iostream>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#include <cstddef>
#include <mutex>

namespace CurrentMetrics
{
extern const Metric DestroyAggregatesThreads;
}

namespace DB
{

struct RuntimeDataflowStatistics
{
    RuntimeDataflowStatistics & operator+=(const RuntimeDataflowStatistics & rhs)
    {
        input_bytes += rhs.input_bytes;
        output_bytes += rhs.output_bytes;
        return *this;
    }

    size_t input_bytes = 0;
    size_t output_bytes = 0;
};

class RuntimeDataflowStatisticsCache
{
public:
    using Entry = RuntimeDataflowStatistics;
    using Cache = DB::CacheBase<UInt64, Entry>;
    using CachePtr = std::shared_ptr<Cache>;

    RuntimeDataflowStatisticsCache()
        : stats_cache(std::make_shared<Cache>(
              CurrentMetrics::DestroyAggregatesThreads, CurrentMetrics::DestroyAggregatesThreads, 1024 * 1024 * 1024, 0))
    {
    }

    std::optional<Entry> getStats([[maybe_unused]] size_t key) const
    {
        std::lock_guard lock(mutex);
        if (const auto entry = stats_cache->get(key))
            return *entry;
        return std::nullopt;
    }

    template <typename Func>
    void update([[maybe_unused]] size_t key, Func foo)
    {
        std::lock_guard lock(mutex);
        if (!stats_cache->contains(key))
            stats_cache->set(key, std::make_shared<Entry>());
        const auto entry = stats_cache->get(key);
        foo(*entry);
        LOG_DEBUG(&Poco::Logger::get("debug"), "entry->input_bytes={}, entry->output_bytes={}", entry->input_bytes, entry->output_bytes);
    }

private:
    mutable std::mutex mutex;
    CachePtr stats_cache TSA_GUARDED_BY(mutex);
};

inline RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache()
{
    static RuntimeDataflowStatisticsCache stats_cache;
    return stats_cache;
}

}
