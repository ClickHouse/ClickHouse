#pragma once

#include <Common/CacheBase.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/Chunk.h>
#include <Storages/ColumnSize.h>

#include <cstddef>
#include <memory>
#include <mutex>

namespace DB
{

class Aggregator;
struct AggregatedDataVariants;

struct RuntimeDataflowStatistics
{
    size_t input_bytes = 0;
    size_t output_bytes = 0;
};

inline RuntimeDataflowStatistics operator+(const RuntimeDataflowStatistics & lhs, const RuntimeDataflowStatistics & rhs)
{
    return RuntimeDataflowStatistics{lhs.input_bytes + rhs.input_bytes, lhs.output_bytes + rhs.output_bytes};
}

class RuntimeDataflowStatisticsCache
{
public:
    using Entry = RuntimeDataflowStatistics;
    using Cache = DB::CacheBase<UInt64, Entry>;
    using CachePtr = std::shared_ptr<Cache>;

    RuntimeDataflowStatisticsCache()
        : stats_cache(std::make_shared<Cache>(CurrentMetrics::end(), CurrentMetrics::end(), 1024 * 1024 * 1024, 0))
    {
    }

    std::optional<Entry> getStats(size_t key) const;

    void update(size_t key, RuntimeDataflowStatistics stats);

private:
    mutable std::mutex mutex;
    CachePtr stats_cache TSA_GUARDED_BY(mutex);
};

RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache();

class RuntimeDataflowStatisticsCacheUpdater
{
    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;

    struct Statistics
    {
        size_t bytes = 0;
        size_t sample_bytes = 0;
        size_t compressed_bytes = 0;
        size_t elapsed_microseconds = 0;
    };

public:
    RuntimeDataflowStatisticsCacheUpdater() = default;

    ~RuntimeDataflowStatisticsCacheUpdater();

    void setCacheKey(size_t key) { cache_key = key; }

    void recordOutputChunk(const Chunk & chunk, const Block & header);

    void recordAggregationStateSizes(AggregatedDataVariants & variant, ssize_t bucket);

    void recordAggregationKeySizes(const Aggregator & aggregator, const Block & block);

    void recordInputColumns(const ColumnsWithTypeAndName & columns, const ColumnSizeByName & column_sizes, size_t bytes);

    void recordInputColumns(const ColumnsWithTypeAndName & columns, const ColumnSizeByName & column_sizes);

private:
    size_t getCompressedColumnSize(const ColumnWithTypeAndName & column);

    std::optional<size_t> cache_key;

    std::atomic_size_t cnt{0};

    std::mutex mutex;

    std::array<Statistics, 2> input_bytes_statistics TSA_GUARDED_BY(mutex){};
    std::array<Statistics, 3> output_bytes_statistics TSA_GUARDED_BY(mutex){};

    bool unsupported_case TSA_GUARDED_BY(mutex) = false;
};

using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;
}
