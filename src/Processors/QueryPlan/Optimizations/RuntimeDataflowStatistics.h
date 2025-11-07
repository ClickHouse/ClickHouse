#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Processors/Chunk.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/CacheBase.h>

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
public:
    explicit RuntimeDataflowStatisticsCacheUpdater(std::optional<size_t> cache_key_)
        : cache_key(cache_key_)
    {
    }

    ~RuntimeDataflowStatisticsCacheUpdater();

    void setHeader(const Block & header_)
    {
        if (!cache_key)
            return;
        header = header_;
    }

    void recordOutputChunk(const Chunk & chunk);

    void recordAggregateFunctionSizes(AggregatedDataVariants & variant, ssize_t bucket);

    void recordAggregationKeySizes(const Aggregator & aggregator, const Block & block);

    void recordInputColumns(const ColumnsWithTypeAndName & columns, const IMergeTreeDataPart::ColumnSizeByName & column_sizes, size_t bytes);

    void recordInputColumns(const ColumnsWithTypeAndName & columns, const IMergeTreeDataPart::ColumnSizeByName & column_sizes);

private:
    size_t compressedColumnSize(const ColumnWithTypeAndName & column);

    const std::optional<size_t> cache_key;
    Block header;

    std::mutex mutex;

    size_t input_bytes_sample = 0;
    size_t input_bytes_compressed = 0;

    size_t output_bytes_sample = 0;
    size_t output_bytes_compressed = 0;
    RuntimeDataflowStatistics statistics{};

    size_t output_bytes_sample2 = 0;
    size_t output_bytes_compressed2 = 0;
    RuntimeDataflowStatistics statistics2{};

    bool unsupported_case = false;

    std::atomic_size_t cnt{0};

    std::array<size_t, 5> elapsed{};
};

using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;
}
