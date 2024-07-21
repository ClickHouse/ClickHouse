#pragma once
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Processors/Formats/Impl/Parquet/SelectiveColumnReader.h>


#include <arrow/io/interfaces.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

namespace DB
{
class ParquetReader
{
public:
    friend class RowGroupChunkReader;
    ParquetReader(
        Block header_,
        std::shared_ptr<ReadBufferFromFileBase> file,
        parquet::ArrowReaderProperties arrow_properties_,
        parquet::ReaderProperties reader_properties_,
        std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
        const FormatSettings & format_settings,
        std::vector<int> row_groups_indices_ = {},
        std::shared_ptr<parquet::FileMetaData> metadata = nullptr);

    Block read();
    void addFilter(const String & column_name, ColumnFilterPtr filter);
private:
    bool loadRowGroupChunkReaderIfNeeded();

    std::unique_ptr<parquet::ParquetFileReader> file_reader;
    std::shared_ptr<ReadBufferFromFileBase> file;
    parquet::ArrowReaderProperties arrow_properties;

    Block header;

    std::unique_ptr<RowGroupChunkReader> row_group_chunk_reader;

    UInt64 max_block_size;
    parquet::ReaderProperties properties;
    std::unordered_map<String, ColumnFilterPtr> filters;
    std::vector<int> parquet_col_indice;
    std::vector<int> row_groups_indices;
    size_t next_row_group_idx = 0;
    std::shared_ptr<parquet::FileMetaData> meta_data;
};

}


