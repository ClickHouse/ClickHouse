#pragma once
#include <memory>
#include <IO/SeekableReadBuffer.h>
#include <Storages/MergeTree/MergeTreeReaderParquet.h>

#if USE_PARQUET

#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <generated/parquet_types.h>

namespace DB
{

class MergeTreeReaderParquetSingleBuffer : public MergeTreeReaderParquet
{
public:
    template <typename... Args>
    explicit MergeTreeReaderParquetSingleBuffer(Args &&... args)
        : MergeTreeReaderParquet{std::forward<Args>(args)...}
    {
    }

    size_t readRows(size_t from_mark, size_t current_task_last_mark,
                    bool continue_reading, size_t max_rows_to_read,
                    size_t rows_offset, Columns & res_columns) override;

private:
    void init();
    void readPage(size_t column_state_idx, size_t mark, ColumnPtr & res_column);
    size_t rowGroupOfMark(size_t mark) const;

    struct ColumnState
    {
        size_t column_idx = 0;
        size_t output_idx = 0;
        Parquet::PageDecoderInfo decoder_info;
        std::vector<parquet::format::OffsetIndex> offset_index;
        std::vector<parquet::format::ColumnMetaData> chunk_meta;
        Parquet::Dictionary dictionary;
    };

    bool initialized = false;
    std::unique_ptr<SeekableReadBuffer> file_buf;
    parquet::format::FileMetaData footer;
    std::vector<size_t> row_group_first_mark;
    std::vector<ColumnState> columns;
};

}

#endif
