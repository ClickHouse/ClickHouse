#pragma once
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Processors/Formats/Impl/Parquet/SelectiveColumnReader.h>
#include <Interpreters/ExpressionActions.h>

#include <arrow/io/interfaces.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

namespace DB
{

class SubRowGroupRangeReader
{
public:
    using RowGroupReaderCreator = std::function<std::unique_ptr<RowGroupChunkReader>(size_t)>;
    explicit SubRowGroupRangeReader(const std::vector<Int32> & rowGroupIndices, RowGroupReaderCreator&& creator);
    DB::Chunk read(size_t rows);

private:
    bool loadRowGroupChunkReaderIfNeeded();

    std::vector<Int32> row_group_indices;
    std::unique_ptr<RowGroupChunkReader> row_group_chunk_reader;
    size_t next_row_group_idx = 0;
    RowGroupReaderCreator row_group_reader_creator;
};

class ParquetReader
{
public:
    friend class RowGroupChunkReader;
    ParquetReader(
        Block header_,
        SeekableReadBuffer& file,
        parquet::ArrowReaderProperties arrow_properties_,
        parquet::ReaderProperties reader_properties_,
        std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
        const FormatSettings & format_settings,
        std::vector<int> row_groups_indices_ = {},
        std::shared_ptr<parquet::FileMetaData> metadata = nullptr);

    Block read();
    void addFilter(const String & column_name, ColumnFilterPtr filter);
    void setRemainFilter(std::optional<ActionsDAG> & expr);
    std::unique_ptr<RowGroupChunkReader> getRowGroupChunkReader(size_t row_group_idx);
    std::unique_ptr<SubRowGroupRangeReader> getSubRowGroupRangeReader(std::vector<Int32> row_group_indices);
private:
    std::unique_ptr<parquet::ParquetFileReader> file_reader;
    std::mutex file_mutex;
    SeekableReadBuffer& file;
    parquet::ArrowReaderProperties arrow_properties;

    Block header;

    std::unique_ptr<SubRowGroupRangeReader> chunk_reader;

    UInt64 max_block_size;
    parquet::ReaderProperties properties;
    std::unordered_map<String, ColumnFilterPtr> filters;
    std::optional<ExpressionActions> remain_filter = std::nullopt;
    std::vector<int> parquet_col_indices;
    std::vector<int> row_groups_indices;
    size_t next_row_group_idx = 0;
    std::shared_ptr<parquet::FileMetaData> meta_data;
};

}


