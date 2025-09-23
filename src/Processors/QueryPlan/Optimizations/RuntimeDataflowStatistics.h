#pragma once

#include <Common/CacheBase.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#include <cstddef>
#include <memory>
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

    std::optional<Entry> getStats(size_t key) const
    {
        std::lock_guard lock(mutex);
        if (const auto entry = stats_cache->get(key))
            return *entry;
        return std::nullopt;
    }

    void update(size_t key, RuntimeDataflowStatistics stats)
    {
        std::lock_guard lock(mutex);
        stats_cache->set(key, std::make_shared<RuntimeDataflowStatistics>(stats));
        LOG_DEBUG(&Poco::Logger::get("debug"), "input_bytes={}, output_bytes={}", stats.input_bytes, stats.output_bytes);
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

struct Updater
{
    explicit Updater(std::optional<size_t> cache_key_)
        : cache_key(cache_key_)
    {
    }

    ~Updater()
    {
        if (!cache_key)
            return;
        auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
        dataflow_cache.update(*cache_key, statistics);
    }

    void operator()(const RuntimeDataflowStatistics & stats)
    {
        if (!cache_key)
            return;
        std::lock_guard lock(mutex);
        statistics += stats;
    }

    std::mutex mutex;
    RuntimeDataflowStatistics statistics{};
    std::optional<size_t> cache_key;
};

using UpdaterPtr = std::shared_ptr<Updater>;
}
