#pragma once

#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnLazy.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ColumnWithTypeAndName.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Chunk.h>
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

namespace ProfileEvents
{
extern const Event RuntimeDataflowStatisticsInputBytes;
extern const Event RuntimeDataflowStatisticsOutputBytes;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct RuntimeDataflowStatistics
{
    size_t input_bytes = 0;
    size_t input_bytes_sample = 0;
    size_t input_bytes_compressed = 0;
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
        ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsInputBytes, stats.input_bytes);
        ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputBytes, stats.output_bytes);
        std::lock_guard lock(mutex);
        if (auto existing_stats = stats_cache->get(key))
        {
            stats.input_bytes = std::max(stats.input_bytes, existing_stats->input_bytes);
            stats.output_bytes = std::max(stats.output_bytes, existing_stats->output_bytes);
        }
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

class Updater
{
public:
    explicit Updater(std::optional<size_t> cache_key_)
        : cache_key(cache_key_)
    {
    }

    ~Updater();

    void setHeader(const Block & header_)
    {
        if (!cache_key)
            return;
        header = header_;
    }

    void addOutputBytes(const Chunk & chunk);

    void addInputBytes(size_t bytes, const Block & block);

    void addInputBytes(const ColumnWithTypeAndName & column);

    void addInputBytes(const Chunk & chunk);

private:
    size_t compressedColumnSize(const ColumnWithTypeAndName & column)
    {
        ColumnBLOB::BLOB blob;
        ColumnBLOB::toBLOB(blob, column, CompressionCodecFactory::instance().get("LZ4", {}), DBMS_TCP_PROTOCOL_VERSION, std::nullopt);
        return blob.size();
    }

    size_t cnt = 0;

    std::mutex mutex;
    RuntimeDataflowStatistics statistics{};
    Block header;
    // bool first_input = true;
    bool first_output = true;
    double output_compression_ratio = 1.0;
    std::optional<size_t> cache_key;
};

using UpdaterPtr = std::shared_ptr<Updater>;
}
