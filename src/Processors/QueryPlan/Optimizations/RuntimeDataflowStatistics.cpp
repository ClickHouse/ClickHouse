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
            static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes),
            stats.elapsed_microseconds);
    };

    RuntimeDataflowStatistics res{.total_rows_to_read = total_rows_to_read};
    for (size_t i = 0; i < InputStatisticsType::MaxInputType; ++i)
    {
        const auto & stats = input_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, toString(static_cast<InputStatisticsType>(i)));
            const auto compression_ratio = static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes);
            res.input_bytes += static_cast<size_t>(static_cast<double>(stats.bytes) / compression_ratio);
        }
    }
    for (size_t i = 0; i < OutputStatisticsType::MaxOutputType; ++i)
    {
        const auto & stats = output_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, toString(static_cast<OutputStatisticsType>(i)));
            const auto compression_ratio = static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes);
            res.output_bytes += static_cast<size_t>(static_cast<double>(stats.bytes) / compression_ratio);
        }
    }

    LOG_DEBUG(
        getLogger("RuntimeDataflowStatisticsCacheUpdater"),
        "Collected statistics: input bytes={}, output bytes={}",
        res.input_bytes,
        res.output_bytes);

    if (res.input_bytes == 0 && res.output_bytes == 0)
    {
        LOG_DEBUG(getLogger("RuntimeDataflowStatisticsCacheUpdater"), "No statistics collected, skipping statistics update.");
        return;
    }

    auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
    dataflow_cache.update(cache_key, res);
}

/// Tries to estimate compressed size of a column by serializing a sample of it.
static std::pair<size_t, size_t> estimateCompressedColumnSize(const ColumnWithTypeAndName & column)
{
    NullWriteBuffer null_buf;
    CompressedWriteBuffer compressed_buf(null_buf);
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    // To avoid spending too much time on serialization, we limit the number of rows to serialize.
    const auto limit = std::max<size_t>(std::min(8192ul, column_to_write->size()), column_to_write->size() / 10);
    NativeWriter::writeData(*serialization, column_to_write, compressed_buf, std::nullopt, 0, limit, DBMS_TCP_PROTOCOL_VERSION);
    compressed_buf.finalize();
    // Return pair of (sample size, compressed size), note that both sizes are based on limited number of rows.
    return std::make_pair(compressed_buf.count(), null_buf.count());
}

bool RuntimeDataflowStatisticsCacheUpdater::shouldSampleBlock(Statistics & statistics, size_t block_rows) const
{
    // Empty blocks produced during planning, when we calculate output headers. Skip them.
    if (!block_rows)
        return false;
    const auto counter = statistics.counter.fetch_add(1, std::memory_order_relaxed);
    return counter % 5 == 0 && counter < 25;
}

void RuntimeDataflowStatisticsCacheUpdater::recordOutputChunk(const Chunk & chunk, const Block & header)
{
    Stopwatch watch;

    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    auto & statistics = output_bytes_statistics[OutputStatisticsType::OutputChunk];
    if (shouldSampleBlock(statistics, chunk.getNumRows()))
    {
        chassert(chunk.getNumColumns() == header.columns());
        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
        {
            auto [sample, compressed] = estimateCompressedColumnSize({chunk.getColumns()[i], header.getByPosition(i).type, ""});
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
    Stopwatch watch;

    /// We want to avoid situations when there is a single very large state (think of `SELECT uniqExact(col) FROM t`).
    /// Then we will spend a lot of time serializing it, and the overhead will be too high.
    if (variant.type == AggregatedDataVariants::Type::without_key
        && std::ranges::any_of(
            variant.aggregator->getParams().aggregates, [](auto agg_func) { return !agg_func.function->hasTrivialDestructor(); }))
    {
        markUnsupportedCase();
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
    Stopwatch watch;

    auto get_key_column_sizes = [&](bool compress)
    {
        size_t sample_bytes = 0;
        size_t compressed_bytes = 0;
        for (size_t i = 0; i < aggregator.getParams().keys_size; ++i)
        {
            const auto & key_column_name = aggregator.getParams().keys[i];
            const auto & column = block.getByName(key_column_name);
            if (compress)
            {
                auto [sample, compressed] = estimateCompressedColumnSize(column);
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

    const auto block_bytes = get_key_column_sizes(/*compressed=*/false).first;
    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    auto & statistics = output_bytes_statistics[OutputStatisticsType::AggregationKeys];
    if (shouldSampleBlock(statistics, block.rows()))
        std::tie(sample_bytes, compressed_bytes) = get_key_column_sizes(/*compressed=*/true);

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
    const ColumnsWithTypeAndName & input_columns,
    const NamesAndTypesList & part_columns,
    const ColumnSizeByName & column_sizes,
    size_t read_bytes,
    std::optional<bool> & should_continue_sampling)
{
    Stopwatch watch;

    const auto type = read_bytes ? InputStatisticsType::WithByteHint : InputStatisticsType::WithoutByteHint;
    if (type == InputStatisticsType::WithoutByteHint)
    {
        for (const auto & column : input_columns)
            read_bytes += column.column->byteSize();
    }

    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    auto & statistics = input_bytes_statistics[type];
    if (read_bytes && !input_columns.empty())
    {
        if (!column_sizes.empty())
        {
            for (const auto & column : input_columns)
            {
                if (column_sizes.contains(column.name))
                {
                    const auto compressed_ratio = column_sizes.at(column.name).data_uncompressed
                        ? (static_cast<double>(column_sizes.at(column.name).data_compressed)
                           / static_cast<double>(column_sizes.at(column.name).data_uncompressed))
                        : 1.0;
                    sample_bytes += column.column->byteSize();
                    compressed_bytes += static_cast<size_t>(static_cast<double>(column.column->byteSize()) * compressed_ratio);
                }
            }
        }
        else
        {
            if (!should_continue_sampling.has_value())
                should_continue_sampling = shouldSampleBlock(statistics, input_columns[0].column->size());

            // We don't have individual column size info, likely because it is a compact part. Let's try to estimate it.
            if (*should_continue_sampling)
            {
                for (const auto & column : input_columns)
                {
                    // Paranoid check in case some, e.g., prewhere filter columns are present among the input columns
                    if (part_columns.contains(column.name))
                    {
                        const auto [sample, compressed] = estimateCompressedColumnSize(column);
                        sample_bytes += sample;
                        compressed_bytes += compressed;
                    }
                }
            }
        }
    }

    std::lock_guard lock(statistics.mutex);
    statistics.bytes += read_bytes;
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
    }
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
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
