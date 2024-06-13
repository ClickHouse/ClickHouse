#pragma once

#include <memory>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReader.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnIndexFilter.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <arrow/io/interfaces.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>


namespace DB
{

class ParquetRecordReader
{
public:
    ParquetRecordReader(
        Block header_,
        parquet::ArrowReaderProperties arrow_properties_,
        parquet::ReaderProperties reader_properties_,
        std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
        const FormatSettings & format_settings,
        std::vector<int> row_groups_indices_,
        const std::vector<int> & column_indices_,
        std::shared_ptr<const KeyCondition> key_condition_,
        std::shared_ptr<parquet::FileMetaData> metadata = nullptr);

    Chunk readChunk();

private:
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file;
    std::unique_ptr<parquet::ParquetFileReader> file_reader;
    parquet::ArrowReaderProperties arrow_properties;

    Block header;

    std::shared_ptr<parquet::RowGroupReader> cur_row_group_reader;
    ParquetColReaders column_readers;

    UInt64 max_block_size;

    std::vector<int> parquet_col_indice;
    std::vector<int> row_groups_indices;
    std::unique_ptr<ParquetFileColumnIndexFilter> column_index_filter;
    //UInt64 cur_row_group_left_rows = 0;
    bool current_row_group_finished = true;
    UInt64 next_row_group_idx = 0;

    Poco::Logger * log;

    void loadNextRowGroup();
    Int64 getTotalRows(const parquet::FileMetaData & meta_data);
    std::unique_ptr<parquet::PageReader> buildPageReader(
        arrow::io::RandomAccessFile & reader,
        const std::vector<arrow::io::ReadRange> & ranges,
        parquet::ColumnChunkMetaData & col_metadata,
        bool always_compressed);
};

}
