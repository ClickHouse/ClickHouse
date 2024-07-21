#include "ParquetReader.h"

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


std::unique_ptr<parquet::ParquetFileReader> createFileReader(
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
    std::shared_ptr<ReadBufferFromFileBase> file_,
    parquet::ArrowReaderProperties arrow_properties_,
    parquet::ReaderProperties reader_properties_,
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file_,
    const FormatSettings & format_settings,
    std::vector<int> row_groups_indices_,
    std::shared_ptr<parquet::FileMetaData> metadata)
    : file_reader(createFileReader(arrow_file_, reader_properties_, metadata))
    , file(file_)
    , arrow_properties(arrow_properties_)
    , header(std::move(header_))
    , max_block_size(format_settings.parquet.max_block_size)
    , properties(reader_properties_)
    , row_groups_indices(std::move(row_groups_indices_))
    , meta_data(file_reader->metadata())
{
    if (row_groups_indices.empty())
        for (int i = 0; i < meta_data->num_row_groups(); i++)
            row_groups_indices.push_back(i);
}

bool ParquetReader::loadRowGroupChunkReaderIfNeeded()
{
    if (row_group_chunk_reader && !row_group_chunk_reader->hasMoreRows() && next_row_group_idx >= row_groups_indices.size())
        return false;
    if ((!row_group_chunk_reader || !row_group_chunk_reader->hasMoreRows()) && next_row_group_idx < row_groups_indices.size())
    {
        row_group_chunk_reader
            = std::make_unique<RowGroupChunkReader>(this, meta_data->RowGroup(row_groups_indices[next_row_group_idx]), filters);
        next_row_group_idx++;
    }
    return true;
}
Block ParquetReader::read()
{
    Chunk chunk;
    while (chunk.getNumRows() == 0)
    {
        if (!loadRowGroupChunkReaderIfNeeded()) break;
        chunk = row_group_chunk_reader->readChunk(max_block_size);
    }
    if (!chunk) return header.cloneEmpty();
    return header.cloneWithColumns(chunk.detachColumns());
}
void ParquetReader::addFilter(const String & column_name, const ColumnFilterPtr filter)
{
    filters[column_name] = filter;
}




}
