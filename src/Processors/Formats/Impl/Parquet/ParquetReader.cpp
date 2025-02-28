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

#define THROW_PARQUET_EXCEPTION(s) \
    do \
    { \
        try \
        { \
            (s); \
        } \
        catch (const ::parquet::ParquetException & e) \
        { \
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Parquet exception: {}", e.what()); \
        } \
    } while (false)


static std::unique_ptr<parquet::ParquetFileReader> createFileReader(
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
    parquet::ReaderProperties reader_properties,
    std::shared_ptr<parquet::FileMetaData> metadata = nullptr)
{
    std::unique_ptr<parquet::ParquetFileReader> res;
    THROW_PARQUET_EXCEPTION(res = parquet::ParquetFileReader::Open(std::move(arrow_file), reader_properties, metadata));
    return res;
}

ParquetReader::ParquetReader(
    Block header_,
    SeekableReadBuffer & file_,
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file_,
    const Settings & settings_,
    std::vector<int> row_groups_indices_,
    std::shared_ptr<parquet::FileMetaData> metadata,
    std::shared_ptr<ThreadPool> io_pool_)
    : file_reader(metadata ? nullptr : createFileReader(arrow_file_, settings_.reader_properties, metadata))
    , file(file_)
    , header(std::move(header_))
    , max_block_size(settings_.format_settings.parquet.max_block_size)
    , row_groups_indices(std::move(row_groups_indices_))
    , meta_data(metadata ? metadata : file_reader->metadata())
    , io_pool(io_pool_)
    , settings(settings_)
{
    if (row_groups_indices.empty())
        for (int i = 0; i < meta_data->num_row_groups(); i++)
            row_groups_indices.push_back(i);
    const auto * root = meta_data->schema()->group_node();
    case_insensitive = settings.format_settings.parquet.case_insensitive_column_matching;
    for (int i = 0; i < root->field_count(); ++i)
    {
        const auto & node = root->field(i);
        auto name = case_insensitive ? Poco::toLower(node->name()) : node->name();
        parquet_columns.emplace(name, node);
    }
    chunk_reader = getSubRowGroupRangeReader(row_groups_indices);
    if (!io_pool)
    {
        io_pool = std::make_shared<ThreadPool>(
            CurrentMetrics::ParquetDecoderIOThreads,
            CurrentMetrics::ParquetDecoderIOThreadsActive,
            CurrentMetrics::ParquetDecoderIOThreadsScheduled,
            1);
    }
}

Block ParquetReader::read()
{
    Chunk chunk = chunk_reader->read(max_block_size);
    if (!chunk)
        return header.cloneEmpty();
    return header.cloneWithColumns(chunk.detachColumns());
}

void ParquetReader::setSourceArrowFile(std::shared_ptr<arrow::io::RandomAccessFile> arrow_file_)
{
    this->arrow_file = arrow_file_;
}

void ParquetReader::addFilter(const String & column_name, const ColumnFilterPtr filter)
{
    //    std::cerr << "add filter to column " << column_name << ": " << filter->toString() << std::endl;
    condition_columns.insert(column_name);
    if (!filters.contains(column_name))
        filters[column_name] = filter;
    else
        filters[column_name] = filters[column_name]->merge(filter.get());
    //    std::cerr << "filter on column " << column_name << ": " << filters[column_name]->toString() << std::endl;
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
        RowGroupPrefetchPtr condition_prefetch = std::make_unique<RowGroupPrefetch>(file, file_mutex, settings.arrow_properties, *io_pool);
        auto row_group_meta = meta_data->RowGroup(row_group_idx);
        row_group_prefetches.push_back(std::move(prefetch));
        row_group_condition_prefetches.push_back(std::move(condition_prefetch));
    }
    return std::make_unique<SubRowGroupRangeReader>(
        row_group_indices_,
        std::move(row_group_condition_prefetches),
        std::move(row_group_prefetches),
        [&](const size_t idx, RowGroupPrefetchPtr condition_prefetch, RowGroupPrefetchPtr prefetch)
        { return getRowGroupChunkReader(idx, std::move(condition_prefetch), std::move(prefetch)); });
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
void ParquetReader::pushDownFilter(FilterSplitResultPtr filter_split_result_)
{
    filter_split_result = filter_split_result_;
    for (const auto & item : filter_split_result->filters)
    {
        addFilter(item.first, item.second);
    }
    for (auto & expression_filter : filter_split_result->expression_filters)
    {
        addExpressionFilter(expression_filter);
    }
}

SubRowGroupRangeReader::SubRowGroupRangeReader(
    const std::vector<Int32> & rowGroupIndices,
    std::vector<RowGroupPrefetchPtr> && row_group_condition_prefetches_,
    std::vector<RowGroupPrefetchPtr> && row_group_prefetches_,
    RowGroupReaderCreator && creator)
    : row_group_indices(rowGroupIndices)
    , row_group_condition_prefetches(std::move(row_group_condition_prefetches_))
    , row_group_prefetches(std::move(row_group_prefetches_))
    , row_group_reader_creator(creator)
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
    if (row_group_chunk_reader && !row_group_chunk_reader->hasMoreRows() && next_row_group_idx >= row_group_indices.size())
        return false;
    if ((!row_group_chunk_reader || !row_group_chunk_reader->hasMoreRows()) && next_row_group_idx < row_group_indices.size())
    {
        row_group_chunk_reader = row_group_reader_creator(
            row_group_indices[next_row_group_idx],
            row_group_condition_prefetches.empty() ? nullptr : std::move(row_group_condition_prefetches[next_row_group_idx]),
            std::move(row_group_prefetches[next_row_group_idx]));
        next_row_group_idx++;
    }
    return true;
}

}
