#pragma once

#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnLazy.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ColumnWithTypeAndName.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
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

namespace DB
{

struct RuntimeDataflowStatistics
{
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

    ~Updater()
    {
        if (!cache_key)
            return;
        statistics.input_bytes = static_cast<size_t>(statistics.input_bytes / input_compression_ratio);
        statistics.output_bytes = static_cast<size_t>(statistics.output_bytes / output_compression_ratio);
        auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
        dataflow_cache.update(*cache_key, statistics);
    }

    void setHeader(const Block & header_)
    {
        if (!cache_key)
            return;
        header = header_;
    }

    void addOutputBytes(const Chunk & chunk)
    {
        if (!cache_key)
            return;
        std::lock_guard lock(mutex);
        const auto source_bytes = chunk.bytes();
        statistics.output_bytes += source_bytes;
        if (first_output && chunk.hasRows())
        {
            if (chunk.getNumColumns() != header.columns())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Chunk columns number {} doesn't match header columns number {}",
                    chunk.getNumColumns(),
                    header.columns());
            }

            size_t compressed_bytes = 0;
            for (size_t i = 0; i < chunk.getNumColumns(); ++i)
                compressed_bytes += compressedColumnSize({chunk.getColumns()[i], header.getByPosition(i).type, ""});
            output_compression_ratio = static_cast<double>(source_bytes) / static_cast<double>(compressed_bytes);
            first_output = false;
        }
    }

    void addInputBytes(const ColumnWithTypeAndName & column)
    {
        if (!cache_key)
            return;
        std::lock_guard lock(mutex);
        const auto source_bytes = column.column->byteSize();
        statistics.input_bytes += source_bytes;
        if (first_input && !column.column->empty())
        {
            const auto compressed_bytes = compressedColumnSize(column);
            input_compression_ratio = static_cast<double>(source_bytes) / static_cast<double>(compressed_bytes);
            first_input = false;
        }
    }

    void addInputBytes(const Chunk & chunk)
    {
        if (!cache_key)
            return;
        std::lock_guard lock(mutex);
        const auto source_bytes = chunk.bytes();
        statistics.input_bytes += source_bytes;
        if (first_input && chunk.hasRows())
        {
            if (chunk.getNumColumns() != header.columns())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Chunk columns number {} doesn't match header columns number {}",
                    chunk.getNumColumns(),
                    header.columns());
            }

            size_t compressed_bytes = 0;
            for (size_t i = 0; i < chunk.getNumColumns(); ++i)
                if (typeid_cast<const ColumnLazy *>(chunk.getColumns()[i].get()) == nullptr)
                    compressed_bytes += compressedColumnSize({chunk.getColumns()[i], header.getByPosition(i).type, ""});
            input_compression_ratio = static_cast<double>(source_bytes) / static_cast<double>(compressed_bytes);
            first_input = false;
        }
    }

private:
    size_t compressedColumnSize(const ColumnWithTypeAndName & column)
    {
        ColumnBLOB::BLOB blob;
        ColumnBLOB::toBLOB(blob, column, CompressionCodecFactory::instance().get("LZ4", {}), DBMS_TCP_PROTOCOL_VERSION, std::nullopt);
        return blob.size();
    }

    std::mutex mutex;
    RuntimeDataflowStatistics statistics{};
    Block header;
    bool first_input = true;
    double input_compression_ratio = 1.0;
    bool first_output = true;
    double output_compression_ratio = 1.0;
    std::optional<size_t> cache_key;
};

using UpdaterPtr = std::shared_ptr<Updater>;
}
