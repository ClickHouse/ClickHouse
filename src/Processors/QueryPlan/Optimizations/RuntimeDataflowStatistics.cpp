#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/NullWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <optional>


namespace ProfileEvents
{
extern const Event RuntimeDataflowStatisticsInputBytes;
extern const Event RuntimeDataflowStatisticsOutputBytes;
}

namespace DB
{

std::optional<RuntimeDataflowStatisticsCache::Entry> RuntimeDataflowStatisticsCache::getStats(size_t key) const
{
    if (const auto entry = stats_cache->get(key))
        return *entry;
    return std::nullopt;
}

void RuntimeDataflowStatisticsCache::update(size_t key, RuntimeDataflowStatistics stats)
{
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsInputBytes, stats.input_bytes);
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputBytes, stats.output_bytes);
    stats_cache->set(key, std::make_shared<RuntimeDataflowStatistics>(stats));
}

RuntimeDataflowStatisticsCacheUpdater::~RuntimeDataflowStatisticsCacheUpdater()
{
    if (!cache_key)
        return;

    if (unsupported_case)
    {
        LOG_DEBUG(getLogger("RuntimeDataflowStatisticsCacheUpdater"), "Unsupported case encountered, skipping statistics update.");
        return;
    }

    auto log_stats = [](const auto & stats, auto type) TSA_REQUIRES(stats.mutex)
    {
        LOG_TEST(
            getLogger("RuntimeDataflowStatisticsCacheUpdater"),
            "{} bytes={}, sample_bytes={}, compressed_bytes={}, compression_ratio={}, elapsed_microseconds={}",
            type,
            stats.bytes,
            stats.sample_bytes,
            stats.compressed_bytes,
            (stats.sample_bytes / 1.0 / stats.compressed_bytes),
            stats.elapsed_microseconds);
    };

    RuntimeDataflowStatistics res;
    for (size_t i = 0; i < InputStatisticsType::MaxInputType; ++i)
    {
        const auto & stats = input_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, toString(static_cast<InputStatisticsType>(i)));
            res.input_bytes += static_cast<size_t>(stats.bytes / (stats.sample_bytes / 1.0 / stats.compressed_bytes));
        }
    }
    for (size_t i = 0; i < OutputStatisticsType::MaxOutputType; ++i)
    {
        const auto & stats = output_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, toString(static_cast<OutputStatisticsType>(i)));
            res.output_bytes += static_cast<size_t>(stats.bytes / (stats.sample_bytes / 1.0 / stats.compressed_bytes));
        }
    }

    LOG_DEBUG(
        getLogger("RuntimeDataflowStatisticsCacheUpdater"),
        "Collected statistics: input bytes={}, output bytes={}",
        res.input_bytes,
        res.output_bytes);

    auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
    dataflow_cache.update(*cache_key, res);
}

void RuntimeDataflowStatisticsCacheUpdater::recordOutputChunk(const Chunk & chunk, const Block & header)
{
    if (!cache_key)
        return;

    Stopwatch watch;

    const auto source_bytes = chunk.bytes();
    size_t key_columns_compressed_size = 0;
    const auto curr = cnt.fetch_add(1, std::memory_order_relaxed);
    if (curr % 50 == 0 && curr < 150 && chunk.hasRows())
    {
        chassert(chunk.getNumColumns() == header.columns());
        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
            key_columns_compressed_size += getCompressedColumnSize({chunk.getColumns()[i], header.getByPosition(i).type, ""});
    }

    auto & statistics = output_bytes_statistics[OutputStatisticsType::OutputChunk];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += source_bytes;
    if (key_columns_compressed_size)
    {
        statistics.sample_bytes += source_bytes;
        statistics.compressed_bytes += key_columns_compressed_size;
    }
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationStateSizes(AggregatedDataVariants & variant, ssize_t bucket)
{
    if (!cache_key)
        return;

    Stopwatch watch;

    if (variant.type == AggregatedDataVariants::Type::without_key
        && std::ranges::any_of(
            variant.aggregator->getParams().aggregates, [](auto agg_func) { return !agg_func.function->hasTrivialDestructor(); }))
    {
        unsupported_case.store(true, std::memory_order_relaxed);
        return;
    }

    size_t res = variant.aggregator->estimateSizeOfCompressedState(variant, bucket);

    auto & statistics = output_bytes_statistics[OutputStatisticsType::AggregationState];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += res;
    statistics.sample_bytes += res;
    statistics.compressed_bytes += res;
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationKeySizes(const Aggregator & aggregator, const Block & block)
{
    if (!cache_key)
        return;

    Stopwatch watch;

    auto getKeyColumnsSize = [&](bool compressed)
    {
        size_t total_size = 0;
        for (size_t i = 0; i < aggregator.getParams().keys_size; ++i)
        {
            const auto & key_column_name = aggregator.getParams().keys[i];
            const auto & column = block.getByName(key_column_name);
            total_size += compressed ? getCompressedColumnSize(column) : column.column->byteSize();
        }
        return total_size;
    };

    const auto source_bytes = getKeyColumnsSize(/*compressed=*/false);
    size_t key_columns_compressed_size = 0;
    const auto curr = cnt.fetch_add(1, std::memory_order_relaxed);
    if (curr % 1 == 0 && curr < 15000000000 && block.rows())
        key_columns_compressed_size = getKeyColumnsSize(/*compressed=*/true);

    auto & statistics = output_bytes_statistics[OutputStatisticsType::AggregationKeys];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += source_bytes;
    if (key_columns_compressed_size)
    {
        statistics.sample_bytes += source_bytes;
        statistics.compressed_bytes += key_columns_compressed_size;
    }
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

void RuntimeDataflowStatisticsCacheUpdater::recordInputColumns(
    const ColumnsWithTypeAndName & columns, const ColumnSizeByName & column_sizes, size_t read_bytes)
{
    if (!cache_key)
        return;

    Stopwatch watch;

    const auto type = read_bytes ? InputStatisticsType::WithByteHint : InputStatisticsType::WithoutByteHint;
    if (type == InputStatisticsType::WithoutByteHint)
    {
        for (const auto & column : columns)
            read_bytes += column.column->byteSize();
    }

    auto & statistics = input_bytes_statistics[type];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += read_bytes;
    if (read_bytes)
    {
        for (const auto & column : columns)
        {
            if (!column_sizes.contains(column.name))
                continue;
            statistics.sample_bytes += column_sizes.at(column.name).data_uncompressed;
            statistics.compressed_bytes += column_sizes.at(column.name).data_compressed;
        }
    }
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

size_t RuntimeDataflowStatisticsCacheUpdater::getCompressedColumnSize(const ColumnWithTypeAndName & column)
{
    NullWriteBuffer wb;
    CompressedWriteBuffer wbuf(wb);
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    NativeWriter::writeData(*serialization, column_to_write, wbuf, std::nullopt, 0, column_to_write->size(), DBMS_TCP_PROTOCOL_VERSION);
    wbuf.finalize();
    return wb.count();
}

RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache()
{
    static RuntimeDataflowStatisticsCache stats_cache;
    return stats_cache;
}
}
