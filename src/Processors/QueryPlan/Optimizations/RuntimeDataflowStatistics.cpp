#include <Compression/CompressedWriteBuffer.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <Interpreters/Aggregator.h>

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
        "statistics.input_bytes={}, input_bytes_sample={}, input_bytes_compressed={}, statistics.output_bytes={}",
        statistics.input_bytes,
        input_bytes_sample,
        input_bytes_compressed,
        statistics.output_bytes);
    if (input_bytes_compressed && (input_bytes_sample / input_bytes_compressed))
        statistics.input_bytes = static_cast<size_t>(statistics.input_bytes / (input_bytes_sample / input_bytes_compressed));
    if (output_bytes_compressed && (output_bytes_sample / output_bytes_compressed))
        statistics.output_bytes = static_cast<size_t>(statistics.output_bytes / (output_bytes_sample / output_bytes_compressed));
    if (output_bytes_compressed2 && (output_bytes_sample2 / output_bytes_compressed2))
        statistics2.output_bytes = static_cast<size_t>(statistics2.output_bytes / (output_bytes_sample2 / output_bytes_compressed2));
    auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
    dataflow_cache.update(*cache_key, statistics + statistics2);
}

void Updater::addOutputBytes(const Chunk & chunk)
{
    if (!cache_key)
        return;

    std::lock_guard lock(mutex);
    const auto source_bytes = chunk.bytes();
    statistics.output_bytes += source_bytes;
    if (cnt++ % 10 == 0 && chunk.hasRows())
    {
        if (chunk.getNumColumns() != header.columns())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Chunk columns number {} doesn't match header columns number {}",
                chunk.getNumColumns(),
                header.columns());
        }

        output_bytes_sample += source_bytes;
        for (size_t i = 0; i < chunk.getNumColumns(); ++i)
            output_bytes_compressed += compressedColumnSize({chunk.getColumns()[i], header.getByPosition(i).type, ""});
    }
}

void Updater::addOutputBytes(const Aggregator &, const ManyAggregatedDataVariants & variants)
{
    if (!cache_key)
        return;

    WriteBufferFromOwnString wb;
    CompressedWriteBuffer wbuf(wb);
    for (const auto & variant : variants)
    {
        if (!variant || !variant->aggregator)
            continue;
        variant->aggregator->applyToAllStates(*variant, wbuf, -1);
    }
    wbuf.finalize();
    // LOG_DEBUG(&Poco::Logger::get("debug"), "wbuf.str());={}", wb.str());
    std::lock_guard lock(mutex);
    statistics.output_bytes += wbuf.count();
    output_bytes_sample += wbuf.count();
    output_bytes_compressed += wbuf.count();
    // LOG_DEBUG(&Poco::Logger::get("debug"), "wbuf.count()={}, cnt={}, sz={}", wbuf.count(), wb.count(), wb.str().size());
}

void Updater::addOutputBytes(const Aggregator &, AggregatedDataVariants & variant, size_t bucket)
{
    if (!cache_key)
        return;

    WriteBufferFromOwnString wb;
    CompressedWriteBuffer wbuf(wb);
    if (!variant.aggregator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "variant.aggregator is nullptr");
    variant.aggregator->applyToAllStates(variant, wbuf, bucket);
    wbuf.finalize();
    // LOG_DEBUG(&Poco::Logger::get("debug"), "wbuf.str());={}", wb.str());
    std::lock_guard lock(mutex);
    statistics.output_bytes += wbuf.count();
    output_bytes_sample += wbuf.count();
    output_bytes_compressed += wbuf.count();
    // LOG_DEBUG(&Poco::Logger::get("debug"), "wbuf.count()={}, cnt={}, sz={}", wbuf.count(), wb.count(), wb.str().size());
}

void Updater::addOutputBytes(const Aggregator & aggregator, const Block & block)
{
    if (!cache_key)
        return;

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

    std::lock_guard lock(mutex);
    const auto source_bytes = getKeyColumnsSize(/*compressed=*/true);
    statistics2.output_bytes += source_bytes;
    if (cnt++ % 1 == 0 && block.rows())
    {
        output_bytes_sample2 += source_bytes;
        output_bytes_compressed2 += getKeyColumnsSize(/*compressed=*/true);
    }
}

void Updater::addInputBytes(const ColumnsWithTypeAndName & columns, const IMergeTreeDataPart::ColumnSizeByName & column_sizes, size_t bytes)
{
    if (!cache_key)
        return;

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
}

void Updater::addInputBytes(const ColumnsWithTypeAndName & columns, const IMergeTreeDataPart::ColumnSizeByName & column_sizes)
{
    if (!cache_key)
        return;

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
}
}
