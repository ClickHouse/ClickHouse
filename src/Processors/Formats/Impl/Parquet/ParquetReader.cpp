#include "ParquetReader.h"
#include <DataTypes/NestedUtils.h>
#include <IO/SharedThreadPools.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilterHelper.h>
#include <arrow/io/util_internal.h>
#include <Poco/String.h>
#include <Common/ThreadPool.h>

namespace CurrentMetrics
{
extern const Metric ParquetDecoderIOThreads;
extern const Metric ParquetDecoderIOThreadsActive;
extern const Metric ParquetDecoderIOThreadsScheduled;
}

namespace DB
{
namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
}

ParquetReader::ParquetReader(
    Block header_,
    SeekableReadBuffer & file_,
    const Settings & settings_,
    const std::shared_ptr<parquet::FileMetaData> & metadata,
    const std::shared_ptr<ThreadPool> & io_pool_)
    :
    file(file_)
    , header(std::move(header_))
    , meta_data(metadata)
    , io_pool(io_pool_)
    , settings(settings_)
{
    const auto * root = meta_data->schema()->group_node();
    case_insensitive = settings.format_settings.parquet.case_insensitive_column_matching;
    for (int i = 0; i < root->field_count(); ++i)
    {
        const auto & node = root->field(i);
        auto name = case_insensitive ? Poco::toLower(node->name()) : node->name();
        parquet_columns.emplace(name, node);
    }
    if (!io_pool)
    {
        io_pool = std::make_shared<ThreadPool>(
            CurrentMetrics::ParquetDecoderIOThreads,
            CurrentMetrics::ParquetDecoderIOThreadsActive,
            CurrentMetrics::ParquetDecoderIOThreadsScheduled,
            1);
    }
}

void ParquetReader::setSourceArrowFile(std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_)
{
    this->arrow_file = arrow_file_;
}

void ParquetReader::addFilter(const String & column_name, const ColumnFilterPtr & filter)
{
    condition_columns.insert(column_name);
    if (!filters.contains(column_name))
        filters[column_name] = filter;
    else
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Filter on column {} already exists.", column_name);
}

std::unique_ptr<RowGroupChunkReader>
ParquetReader::getRowGroupChunkReader(size_t row_group_idx, RowGroupPrefetchPtr conditions_prefetch, RowGroupPrefetchPtr prefetch)
{
    return std::make_unique<RowGroupChunkReader>(this, row_group_idx, std::move(conditions_prefetch), std::move(prefetch), filters);
}

extern arrow::io::ReadRange getColumnRange(const parquet::ColumnChunkMetaData & column_metadata);


std::unique_ptr<SubRowGroupRangeReader> ParquetReader::getSubRowGroupRangeReader(std::vector<Int32> row_group_indices_)
{
    std::vector<RowGroupPrefetchPtr> row_group_prefetches;
    std::vector<RowGroupPrefetchPtr> row_group_condition_prefetches;
    for (auto row_group_idx : row_group_indices_)
    {
        RowGroupPrefetchPtr prefetch = std::make_shared<RowGroupPrefetch>(file, file_mutex, settings.arrow_properties, *io_pool);
        RowGroupPrefetchPtr condition_prefetch = std::make_shared<RowGroupPrefetch>(file, file_mutex, settings.arrow_properties, *io_pool);
        auto row_group_meta = meta_data->RowGroup(row_group_idx);
        row_group_prefetches.push_back(std::move(prefetch));
        row_group_condition_prefetches.push_back(std::move(condition_prefetch));
    }
    return std::make_unique<SubRowGroupRangeReader>(
        row_group_indices_,
        std::move(row_group_condition_prefetches),
        std::move(row_group_prefetches),
        *this
        );
}
void ParquetReader::addExpressionFilter(std::shared_ptr<ExpressionFilter> filter)
{
    if (!filter)
        return;
    for (const auto & item : filter->getInputs())
    {
        condition_columns.insert(item);
    }
    expression_filters.emplace_back(filter);
}
parquet::schema::NodePtr ParquetReader::getParquetColumn(const String & column_name)
{
    auto normalized_name = case_insensitive ? Poco::toLower(column_name) : column_name;
    if (parquet_columns.contains(normalized_name))
        return parquet_columns[normalized_name];
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "no column with '{}' in parquet file", column_name);
}
void ParquetReader::pushDownFilter(const FilterSplitResultPtr & filter_split_result_)
{
    filter_split_result = filter_split_result_;
    for (const auto & item : filter_split_result->filters)
    {
        addFilter(item.first, item.second);
    }
    for (const auto & expression_filter : filter_split_result->expression_filters)
    {
        addExpressionFilter(expression_filter);
    }
}

SubRowGroupRangeReader::SubRowGroupRangeReader(
    const std::vector<Int32> & rowGroupIndices,
    std::vector<RowGroupPrefetchPtr> && row_group_condition_prefetches_,
    std::vector<RowGroupPrefetchPtr> && row_group_prefetches_,
    const ParquetReader & reader_)
    : row_group_indices(rowGroupIndices)
    , row_group_condition_prefetches(std::move(row_group_condition_prefetches_))
    , row_group_prefetches(std::move(row_group_prefetches_))
    , reader(reader_)
{
    if (row_group_indices.size() != row_group_prefetches.size())
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "row group indices and prefetches size mismatch");
}
DB::Chunk SubRowGroupRangeReader::read(size_t rows)
{
    Chunk chunk;
    while (chunk.getNumRows() == 0)
    {
        if (!loadRowGroupChunkReaderIfNeeded())
            break;
        chunk = row_group_chunk_reader->readChunk(rows);
    }
    return chunk;
}
bool SubRowGroupRangeReader::loadRowGroupChunkReaderIfNeeded()
{
    if (row_group_chunk_reader && row_group_chunk_reader->hasMoreRows())
        return true;
    if (next_row_group_idx >= row_group_indices.size())
        return false;
    row_group_chunk_reader = reader.getRowGroupChunkReader(
        row_group_indices.at(next_row_group_idx),
        row_group_condition_prefetches.empty() ? nullptr : std::move(row_group_condition_prefetches.at(next_row_group_idx)),
        std::move(row_group_prefetches.at(next_row_group_idx)));
    next_row_group_idx++;
    chassert(row_group_chunk_reader->hasMoreRows());
    return true;
}

}
