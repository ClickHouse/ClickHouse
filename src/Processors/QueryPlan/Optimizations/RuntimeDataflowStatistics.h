#pragma once

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnSize.h>
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
    CachePtr stats_cache;
};

RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache();

class RuntimeDataflowStatisticsCacheUpdater
{
    using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;

    struct Statistics
    {
        std::atomic_size_t counter{0};

        std::mutex mutex;
        size_t bytes TSA_GUARDED_BY(mutex) = 0;
        size_t sample_bytes TSA_GUARDED_BY(mutex) = 0;
        size_t compressed_bytes TSA_GUARDED_BY(mutex) = 0;
        size_t elapsed_microseconds TSA_GUARDED_BY(mutex) = 0;
    };

public:
    RuntimeDataflowStatisticsCacheUpdater() = default;

    ~RuntimeDataflowStatisticsCacheUpdater();

    void setCacheKey(size_t key) { cache_key = key; }

    void recordOutputChunk(const Chunk & chunk, const Block & header);

    void recordAggregationStateSizes(AggregatedDataVariants & variant, ssize_t bucket);

    void recordAggregationKeySizes(const Aggregator & aggregator, const Block & block);

    void recordInputColumns(const ColumnsWithTypeAndName & columns, const ColumnSizeByName & column_sizes, size_t read_bytes = 0);

private:
    std::optional<size_t> cache_key;

    std::atomic_bool unsupported_case{false};

    enum InputStatisticsType
    {
        WithByteHint = 0,
        WithoutByteHint = 1,
        MaxInputType = 2,
    };
    std::array<Statistics, 2> input_bytes_statistics;

    enum OutputStatisticsType
    {
        AggregationState = 0,
        AggregationKeys = 1,
        OutputChunk = 2,
        MaxOutputType = 3,
    };
    std::array<Statistics, 3> output_bytes_statistics;
};

using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

class RuntimeDataflowStatisticsCollector : public ISimpleTransform
{
public:
    RuntimeDataflowStatisticsCollector(SharedHeader header_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "RuntimeDataflowStatisticsCollector"; }

protected:
    void transform(Chunk & chunk) override;

private:
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
};
}
