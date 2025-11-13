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

    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    auto & statistics = output_bytes_statistics[OutputStatisticsType::OutputChunk];
    const auto counter = statistics.counter.fetch_add(1, std::memory_order_relaxed);
    if (chunk.hasRows() && counter % 50 == 0 && counter < 150)
    {
        chassert(chunk.getNumColumns() == header.columns());
        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
        {
            auto [sample, compressed] = getCompressedColumnSize({chunk.getColumns()[i], header.getByPosition(i).type, ""});
            sample_bytes += sample;
            compressed_bytes += compressed;
        }
    }

    std::lock_guard lock(statistics.mutex);
    statistics.bytes += chunk.bytes();
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
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

    auto getKeyColumnsSize = [&](bool compress)
    {
        size_t sample_bytes = 0;
        size_t compressed_bytes = 0;
        for (size_t i = 0; i < aggregator.getParams().keys_size; ++i)
        {
            const auto & key_column_name = aggregator.getParams().keys[i];
            const auto & column = block.getByName(key_column_name);
            if (compress)
            {
                auto [sample, compressed] = getCompressedColumnSize(column);
                sample_bytes += sample;
                compressed_bytes += compressed;
            }
            else
            {
                sample_bytes += column.column->byteSize();
                compressed_bytes += column.column->byteSize();
            }
        }
        return std::make_pair(sample_bytes, compressed_bytes);
    };

    const auto block_bytes = getKeyColumnsSize(/*compressed=*/false).first;
    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    auto & statistics = output_bytes_statistics[OutputStatisticsType::AggregationKeys];
    const auto counter = statistics.counter.fetch_add(1, std::memory_order_relaxed);
    if (block.rows() && counter % 50 == 0 && counter < 150)
        std::tie(sample_bytes, compressed_bytes) = getKeyColumnsSize(/*compressed=*/true);

    std::lock_guard lock(statistics.mutex);
    statistics.bytes += block_bytes;
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
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

std::pair<size_t, size_t> RuntimeDataflowStatisticsCacheUpdater::getCompressedColumnSize(const ColumnWithTypeAndName & column)
{
    NullWriteBuffer null_buf;
    CompressedWriteBuffer compressed_buf(null_buf);
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    const auto limit = std::max<size_t>(std::min(8192ul, column_to_write->size()), column_to_write->size() / 10);
    NativeWriter::writeData(*serialization, column_to_write, compressed_buf, std::nullopt, 0, limit, DBMS_TCP_PROTOCOL_VERSION);
    compressed_buf.finalize();
    return std::make_pair(compressed_buf.count(), null_buf.count());
}

RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache()
{
    static RuntimeDataflowStatisticsCache stats_cache;
    return stats_cache;
}

RuntimeDataflowStatisticsCollector::RuntimeDataflowStatisticsCollector(
    SharedHeader header_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : ISimpleTransform(header_, header_, /*skip_empty_chunks=*/false)
    , updater(std::move(updater_))
{
}

void RuntimeDataflowStatisticsCollector::transform(Chunk & chunk)
{
    if (updater)
        updater->recordOutputChunk(chunk, getOutputPort().getHeader());
}
}
