#include "ParquetReader.h"
#include <IO/SharedThreadPools.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <arrow/io/util_internal.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int PARQUET_EXCEPTION;
extern const int ARGUMENT_OUT_OF_BOUND;
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
    SeekableReadBuffer& file_,
    parquet::ArrowReaderProperties arrow_properties_,
    parquet::ReaderProperties reader_properties_,
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file_,
    const FormatSettings & format_settings,
    std::vector<int> row_groups_indices_,
    std::shared_ptr<parquet::FileMetaData> metadata)
    : file_reader(metadata ? nullptr : createFileReader(arrow_file_, reader_properties_, metadata))
    , file(file_)
    , arrow_properties(arrow_properties_)
    , header(std::move(header_))
    , max_block_size(format_settings.parquet.max_block_size)
    , properties(reader_properties_)
    , row_groups_indices(std::move(row_groups_indices_))
    , meta_data(metadata ? metadata : file_reader->metadata())
{
    if (row_groups_indices.empty())
        for (int i = 0; i < meta_data->num_row_groups(); i++)
            row_groups_indices.push_back(i);
    const auto * root = meta_data->schema()->group_node();
    for (int i = 0; i < root->field_count(); ++i)
    {
        const auto & node = root->field(i);
        parquet_columns.emplace(node->name(), node);
    }
    chunk_reader = getSubRowGroupRangeReader(row_groups_indices);
}

Block ParquetReader::read()
{
    Chunk chunk = chunk_reader->read(max_block_size);
    if (!chunk) return header.cloneEmpty();
    return header.cloneWithColumns(chunk.detachColumns());
}
void ParquetReader::addFilter(const String & column_name, const ColumnFilterPtr filter)
{
    std::cerr << "add filter to column " << column_name << ": " << filter->toString() << std::endl;
    if (!filters.contains(column_name))
        filters[column_name] = filter;
    else
        filters[column_name] = filters[column_name]->merge(filter.get());
    std::cerr << "filter on column " << column_name << ": " << filters[column_name]->toString() << std::endl;
}
void ParquetReader::setRemainFilter(std::optional<ActionsDAG> & expr)
{
    if (expr.has_value())
    {
        ExpressionActionsSettings settings;
        ExpressionActions actions = ExpressionActions(std::move(expr.value()), settings);
        remain_filter = std::optional<ExpressionActions>(std::move(actions));
    }
}
std::unique_ptr<RowGroupChunkReader> ParquetReader::getRowGroupChunkReader(size_t row_group_idx, RowGroupPrefetchPtr prefetch)
{
    return std::make_unique<RowGroupChunkReader>(this, row_group_idx, std::move(prefetch), filters);
}

extern arrow::io::ReadRange getColumnRange(const parquet::ColumnChunkMetaData & column_metadata);


std::unique_ptr<SubRowGroupRangeReader> ParquetReader::getSubRowGroupRangeReader(std::vector<Int32> row_group_indices_)
{
    std::vector<RowGroupPrefetchPtr> row_group_prefetches;
    for (auto row_group_idx : row_group_indices_)
    {
        RowGroupPrefetchPtr prefetch = std::make_unique<RowGroupPrefetch>(file, file_mutex, arrow_properties);
        auto row_group_meta = meta_data->RowGroup(row_group_idx);
        for (const auto & name : header.getNames())
        {
            if (!parquet_columns.contains(name))
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "no column with '{}' in parquet file", name);
            auto idx = meta_data->schema()->ColumnIndex(*parquet_columns[name]);
            auto range = getColumnRange(*row_group_meta->ColumnChunk(idx));
            prefetch->prefetchRange(range);
        }
        row_group_prefetches.push_back(std::move(prefetch));
    }
    return std::make_unique<SubRowGroupRangeReader>(row_group_indices_, std::move(row_group_prefetches), [&](const size_t idx, RowGroupPrefetchPtr prefetch) { return getRowGroupChunkReader(idx, std::move(prefetch)); });
}


SubRowGroupRangeReader::SubRowGroupRangeReader(const std::vector<Int32> & rowGroupIndices, std::vector<RowGroupPrefetchPtr>&& row_group_prefetches_, RowGroupReaderCreator && creator)
    : row_group_indices(rowGroupIndices), row_group_prefetches(std::move(row_group_prefetches_)), row_group_reader_creator(creator)
{
    if (row_group_indices.size() != row_group_prefetches.size())
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "row group indices and prefetches size mismatch");
}
DB::Chunk SubRowGroupRangeReader::read(size_t rows)
{
    Chunk chunk;
    while (chunk.getNumRows() == 0)
    {
        if (!loadRowGroupChunkReaderIfNeeded()) break;
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
        if (next_row_group_idx == 0) row_group_prefetches.front()->startPrefetch();
        row_group_chunk_reader = row_group_reader_creator(row_group_indices[next_row_group_idx], std::move(row_group_prefetches[next_row_group_idx]));
        next_row_group_idx++;
        if (next_row_group_idx < row_group_indices.size())
            row_group_prefetches[next_row_group_idx]->startPrefetch();
    }
    return true;
}

}
