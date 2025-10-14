#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

namespace DB
{

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
    auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
    dataflow_cache.update(*cache_key, statistics);
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
    WriteBufferFromOwnString wbuf;
    for (const auto & variant : variants)
    {
        if (!variant || !variant->aggregator)
            continue;
        variant->aggregator->applyToAllStates(*variant, wbuf);
    }
    std::lock_guard lock(mutex);
    statistics.output_bytes += wbuf.count();
    output_bytes_sample += wbuf.count();
    output_bytes_compressed += wbuf.count();
    LOG_DEBUG(&Poco::Logger::get("debug"), "wbuf.count()={}", wbuf.count());
}

void Updater::addInputBytes(const IMergeTreeDataPart::ColumnSizeByName & column_sizes, const Block & block, size_t bytes)
{
    if (!cache_key)
        return;

    std::lock_guard lock(mutex);
    statistics.input_bytes += bytes;
    if (bytes)
    {
        for (const auto & column : block.getColumnsWithTypeAndName())
        {
            if (!column_sizes.contains(column.name))
                continue;
            input_bytes_sample += column_sizes.at(column.name).data_uncompressed;
            input_bytes_compressed += column_sizes.at(column.name).data_compressed;
        }
    }
}

void Updater::addInputBytes(const IMergeTreeDataPart::ColumnSizeByName & column_sizes, const ColumnWithTypeAndName & column)
{
    if (!cache_key)
        return;

    std::lock_guard lock(mutex);
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
