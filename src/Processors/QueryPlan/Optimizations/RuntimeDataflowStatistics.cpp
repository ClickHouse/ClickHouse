#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

namespace DB
{

Updater::~Updater()
{
    if (!cache_key)
        return;
    LOG_DEBUG(
        &Poco::Logger::get("debug"),
        "statistics.input_bytes={}, statistics.input_bytes_sample={}, statistics.input_bytes_compressed={}, statistics.output_bytes={}",
        statistics.input_bytes,
        statistics.input_bytes_sample,
        statistics.input_bytes_compressed,
        statistics.output_bytes);
    if (statistics.input_bytes_compressed)
        statistics.input_bytes
            = static_cast<size_t>(statistics.input_bytes / (statistics.input_bytes_sample / statistics.input_bytes_compressed));
    statistics.output_bytes = static_cast<size_t>(statistics.output_bytes / output_compression_ratio);
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
            statistics.input_bytes_sample += column_sizes.at(column.name).data_uncompressed;
            statistics.input_bytes_compressed += column_sizes.at(column.name).data_compressed;
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
        statistics.input_bytes_sample += column_sizes.at(column.name).data_uncompressed;
        statistics.input_bytes_compressed += column_sizes.at(column.name).data_compressed;
    }
}

}
