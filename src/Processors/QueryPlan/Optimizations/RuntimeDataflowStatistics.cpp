#include <algorithm>
#include <Compression/CompressedWriteBuffer.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <Interpreters/Aggregator.h>

#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

Updater::~Updater()
{
    if (!cache_key)
        return;
    LOG_DEBUG(
        &Poco::Logger::get("debug"),
        "statistics.input_bytes={}, input_bytes_sample={}, input_bytes_compressed={}, statistics.output_bytes={}, elapsed=[{}]",
        statistics.input_bytes,
        input_bytes_sample,
        input_bytes_compressed,
        statistics.output_bytes,
        fmt::join(elapsed, ","));
    if (input_bytes_compressed && (input_bytes_sample / input_bytes_compressed))
        statistics.input_bytes = static_cast<size_t>(statistics.input_bytes / (input_bytes_sample / input_bytes_compressed));
    if (output_bytes_compressed && (output_bytes_sample / output_bytes_compressed))
        statistics.output_bytes = static_cast<size_t>(statistics.output_bytes / (output_bytes_sample / output_bytes_compressed));
    if (output_bytes_compressed2 && (output_bytes_sample2 / output_bytes_compressed2))
        statistics2.output_bytes = static_cast<size_t>(statistics2.output_bytes / (output_bytes_sample2 / output_bytes_compressed2));
    if (unsupported_case)
        statistics.output_bytes = (1ULL << 60);
    auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
    dataflow_cache.update(*cache_key, statistics + statistics2);
}

void Updater::addOutputBytes(const Chunk & chunk)
{
    if (!cache_key)
        return;

    Stopwatch watch;

    const auto source_bytes = chunk.bytes();
    size_t key_columns_compressed_size = 0;
    const auto curr = cnt.fetch_add(1, std::memory_order_relaxed);
    if (curr % 50 == 0 && curr < 150 && chunk.hasRows())
    {
        if (chunk.getNumColumns() != header.columns())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Chunk columns number {} doesn't match header columns number {}",
                chunk.getNumColumns(),
                header.columns());
        }

        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
            key_columns_compressed_size += compressedColumnSize({chunk.getColumns()[i], header.getByPosition(i).type, ""});
    }

    std::lock_guard lock(mutex);
    statistics.output_bytes += source_bytes;
    if (key_columns_compressed_size)
    {
        output_bytes_sample += source_bytes;
        output_bytes_compressed += key_columns_compressed_size;
    }

    elapsed[0] += watch.elapsedMicroseconds();
}

void Updater::addOutputBytes(const Aggregator &, AggregatedDataVariants & variant, ssize_t bucket)
{
    if (!cache_key)
        return;

    Stopwatch watch;

    if (bucket == -1
        && std::ranges::any_of(
            variant.aggregator->getParams().aggregates, [](auto agg_func) { return !agg_func.function->hasTrivialDestructor(); }))
    {
        std::lock_guard lock(mutex);
        unsupported_case = true;
        return;
    }

    size_t res = variant.aggregator->applyToAllStates(variant, bucket);

    std::lock_guard lock(mutex);
    statistics.output_bytes += res;
    output_bytes_sample += res;
    output_bytes_compressed += res;

    // LOG_DEBUG(&Poco::Logger::get("debug"), "watch.elapsedMicroseconds());={}", watch.elapsedMicroseconds());
    elapsed[1] += watch.elapsedMicroseconds();
}

void Updater::addOutputBytes(const Aggregator & aggregator, const Block & block)
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
            total_size += compressed ? compressedColumnSize(column) : column.column->byteSize();
        }
        return total_size;
    };

    const auto source_bytes = getKeyColumnsSize(/*compressed=*/false);
    size_t key_columns_compressed_size = 0;
    const auto curr = cnt.fetch_add(1, std::memory_order_relaxed);
    if (curr % 50 == 0 && curr < 150 && block.rows())
        key_columns_compressed_size = getKeyColumnsSize(/*compressed=*/true);

    std::lock_guard lock(mutex);
    statistics2.output_bytes += source_bytes;
    if (key_columns_compressed_size)
    {
        output_bytes_sample2 += source_bytes;
        output_bytes_compressed2 += key_columns_compressed_size;
    }

    elapsed[2] += watch.elapsedMicroseconds();
}

void Updater::addInputBytes(const ColumnsWithTypeAndName & columns, const IMergeTreeDataPart::ColumnSizeByName & column_sizes, size_t bytes)
{
    if (!cache_key)
        return;

    Stopwatch watch;

    std::lock_guard lock(mutex);
    statistics.input_bytes += bytes;
    if (bytes)
    {
        for (const auto & column : columns)
        {
            if (!column_sizes.contains(column.name))
                continue;
            input_bytes_sample += column_sizes.at(column.name).data_uncompressed;
            input_bytes_compressed += column_sizes.at(column.name).data_compressed;
        }
    }

    elapsed[3] += watch.elapsedMicroseconds();
}

void Updater::addInputBytes(const ColumnsWithTypeAndName & columns, const IMergeTreeDataPart::ColumnSizeByName & column_sizes)
{
    if (!cache_key)
        return;

    Stopwatch watch;

    std::lock_guard lock(mutex);
    for (const auto & column : columns)
    {
        statistics.input_bytes += column.column->byteSize();
        if (!column.column->empty())
        {
            if (!column_sizes.contains(column.name))
                return;
            input_bytes_sample += column_sizes.at(column.name).data_uncompressed;
            input_bytes_compressed += column_sizes.at(column.name).data_compressed;
        }
    }

    elapsed[4] += watch.elapsedMicroseconds();
}
}
