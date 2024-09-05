#include "ParquetReader.h"
#include <IO/SharedThreadPools.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
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
    chunk_reader = std::make_unique<SubRowGroupRangeReader>(row_groups_indices, [&](const size_t idx) { return getRowGroupChunkReader(idx); });
    async_downloader = std::make_unique<FileAsyncDownloader>(file, file_mutex);
    const auto * root = meta_data->schema()->group_node();
    for (int i = 0; i < root->field_count(); ++i)
    {
        const auto & node = root->field(i);
        parquet_columns.emplace(node->name(), node);
    }
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
std::unique_ptr<RowGroupChunkReader> ParquetReader::getRowGroupChunkReader(size_t row_group_idx)
{
    return std::make_unique<RowGroupChunkReader>(this, row_group_idx, filters);
}

extern std::pair<size_t, size_t> getColumnRange(const parquet::ColumnChunkMetaData & column_metadata);


std::unique_ptr<SubRowGroupRangeReader> ParquetReader::getSubRowGroupRangeReader(std::vector<Int32> row_group_indices_)
{

    for (auto row_group_idx : row_group_indices_)
    {
        auto row_group_meta = meta_data->RowGroup(row_group_idx);
        for (const auto & name : header.getNames())
        {
            if (!parquet_columns.contains(name))
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "no column with '{}' in parquet file", name);
            auto idx = meta_data->schema()->ColumnIndex(*parquet_columns[name]);
            auto range = getColumnRange(*row_group_meta->ColumnChunk(idx));
            async_downloader->downloadColumnChunkData(row_group_idx, name, range.first, range.second);
        }
    }
    return std::make_unique<SubRowGroupRangeReader>(row_group_indices_, [&](const size_t idx) { return getRowGroupChunkReader(idx); });
}


SubRowGroupRangeReader::SubRowGroupRangeReader(const std::vector<Int32> & rowGroupIndices, RowGroupReaderCreator && creator)
    : row_group_indices(rowGroupIndices), row_group_reader_creator(creator)
{
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
        row_group_chunk_reader = row_group_reader_creator(row_group_indices[next_row_group_idx]);
        next_row_group_idx++;
    }
    return true;
}

FileAsyncDownloader::FileAsyncDownloader(SeekableReadBuffer & file_, std::mutex & mutex) : file(file_), file_mutex(mutex)
{
   callback_runner = threadPoolCallbackRunnerUnsafe<ColumnChunkDataPtr>(getIOThreadPool().get(), "ParquetRead");
}


void FileAsyncDownloader::downloadColumnChunkData(int row_group_idx, const String & column_name, size_t offset, size_t len)
{
    auto task = [this, offset, len]() -> ColumnChunkDataPtr {
        ColumnChunkDataPtr data = std::make_unique<ColumnChunkData>();
        data->data.resize(len);
        size_t count = 0;
        if (file.supportsReadAt())
        {
            auto pb = [](size_t) { return true; };
            count = file.readBigAt(reinterpret_cast<char *>(data->data.data()), len, offset, pb);
        }
        else
        {
            std::lock_guard lock(file_mutex);
            file.seek(offset, SEEK_SET);
            count = file.readBig(reinterpret_cast<char *>(data->data.data()), len);

        }
        if (count != len)
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Failed to read column data");
        return data;
    };

    // Assuming Priority is an enum or a type that is defined elsewhere
    Priority priority = {0}; // Set the appropriate priority value
    auto future =  callback_runner(std::move(task), priority);
    std::lock_guard lock(chunks_mutex);
    column_chunks[row_group_idx][column_name] = std::move(future);
}
std::shared_ptr<ColumnChunkData>
FileAsyncDownloader::readColumnChunkData(int row_group_idx, const String & column_name)
{
    std::lock_guard lock(chunks_mutex);
    if (column_chunks.contains(row_group_idx) && column_chunks[row_group_idx].contains(column_name))
    {
        auto data = std::move(column_chunks[row_group_idx][column_name]);
        return data.get();
    }
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Column chunk data not found {}, {}", row_group_idx, column_name);
}


}
