#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReader.h>

#include <arrow/io/interfaces.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

#include "ParquetColumnReader.h"

namespace DB
{
struct PrewhereInfo;
struct PrewhereExprInfo;

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
        std::shared_ptr<parquet::FileMetaData> metadata = nullptr,
        std::shared_ptr<PrewhereInfo> prewhere_info_ = nullptr);

    ~ParquetRecordReader();

    Chunk readChunk();

private:
    std::unique_ptr<parquet::ParquetFileReader> file_reader;
    parquet::ArrowReaderProperties arrow_properties;

    Block header;

    std::shared_ptr<parquet::RowGroupReader> cur_row_group_reader;

    UInt64 max_block_size;

    std::vector<int> row_groups_indices;
    UInt64 cur_row_group_left_rows = 0;
    int next_row_group_idx = 0;

    Poco::Logger * log;

    struct ChunkReader
    {
        Block sample_block;
        std::vector<int> col_indice;
        ParquetColReaders column_readers;

        void init(ParquetRecordReader &record_reader);
        Columns readBatch(size_t num_rows, const IColumn::Filter * filter);
        void skip(size_t num_rows);
    };

    ChunkReader active_chunk_reader;
    ChunkReader lazy_chunk_reader;
    std::shared_ptr<PrewhereInfo> prewhere_info;
    std::shared_ptr<PrewhereExprInfo> prewhere_actions;

    bool loadNextRowGroup();
    Int64 getTotalRows(const parquet::FileMetaData & meta_data);
};

}
