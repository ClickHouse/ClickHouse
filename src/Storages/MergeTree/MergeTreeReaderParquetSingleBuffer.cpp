#include "config.h"

#include <Storages/MergeTree/MergeTreeReaderParquetSingleBuffer.h>

#if USE_PARQUET

#include <Storages/MergeTree/MergeTreeDataPartParquet.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Processors/Formats/Impl/Parquet/ReadCommon.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <Core/Block.h>
#include <algorithm>
#include <span>
#include <vector>

namespace DB
{

void MergeTreeReaderParquetSingleBuffer::init()
{
    const String file_name = MergeTreeDataPartParquet::DATA_FILE_NAME_WITH_EXTENSION;
    auto & data_part_storage = *data_part_info_for_read->getDataPartStorage();
    const size_t file_size = data_part_storage.getFileSize(file_name);
    file_buf = data_part_storage.readFile(file_name, settings.read_settings, std::nullopt);

    file_buf->seek(file_size - 8, SEEK_SET);
    UInt32 footer_len = 0;
    readBinaryLittleEndian(footer_len, *file_buf);

    std::vector<char> footer_bytes(footer_len);
    file_buf->seek(file_size - 8 - footer_len, SEEK_SET);
    file_buf->readStrict(footer_bytes.data(), footer_len);
    Parquet::deserializeThriftStruct(footer, footer_bytes.data(), footer_len);

    Block sample;
    for (const auto & column : getColumnsToRead())
        sample.insert(ColumnWithTypeAndName(column.type, column.name));

    Parquet::ReadOptions read_options;
    read_options.format = getFormatSettings(data_part_info_for_read->getContext());

    Parquet::SchemaConverter converter(footer, read_options, &sample);
    converter.prepareForReading();

    const size_t num_row_groups = footer.row_groups.size();
    for (auto & primitive : converter.primitive_columns)
    {
        ColumnState state;
        state.column_idx = primitive.column_idx;
        state.output_idx = primitive.idx_in_output_block;
        state.decoder_info = primitive.decoder;
        state.offset_index.resize(num_row_groups);
        state.chunk_meta.resize(num_row_groups);

        for (size_t rg = 0; rg < num_row_groups; ++rg)
        {
            const auto & chunk = footer.row_groups[rg].columns.at(state.column_idx);
            state.chunk_meta[rg] = chunk.meta_data;

            std::vector<char> oi_bytes(chunk.offset_index_length);
            file_buf->seek(chunk.offset_index_offset, SEEK_SET);
            file_buf->readStrict(oi_bytes.data(), oi_bytes.size());
            Parquet::deserializeThriftStruct(state.offset_index[rg], oi_bytes.data(), oi_bytes.size());
        }
        columns.push_back(std::move(state));
    }

    row_group_first_mark.push_back(0);
    for (size_t row_group_idx = 0; row_group_idx < num_row_groups; ++row_group_idx)
    {
        const size_t pages = columns.empty() ? 0 : columns[0].offset_index[row_group_idx].page_locations.size();
        row_group_first_mark.push_back(row_group_first_mark.back() + pages);
    }

    initialized = true;
}

size_t MergeTreeReaderParquetSingleBuffer::rowGroupOfMark(size_t mark) const
{
    return std::upper_bound(row_group_first_mark.begin(), row_group_first_mark.end(), mark)
        - row_group_first_mark.begin() - 1;
}

void MergeTreeReaderParquetSingleBuffer::readPage(size_t column_state_idx, size_t mark, ColumnPtr & res_column)
{
    ColumnState & state = columns[column_state_idx];
    const size_t rg = rowGroupOfMark(mark);
    const size_t page = mark - row_group_first_mark[rg];
    const auto & loc = state.offset_index[rg].page_locations.at(page);

    file_buf->seek(loc.offset, SEEK_SET);
    std::vector<char> raw(loc.compressed_page_size);
    file_buf->readStrict(raw.data(), raw.size());

    parquet::format::PageHeader header;
    const size_t header_size = Parquet::deserializeThriftStruct(header, raw.data(), raw.size());

    const char * body = raw.data() + header_size;
    const size_t num_values = static_cast<size_t>(header.data_page_header.num_values);

    auto decoder = state.decoder_info.makeDecoder(
        header.data_page_header.encoding,
        std::span<const char>(body, static_cast<size_t>(header.uncompressed_page_size)));

    auto mutable_column = IColumn::mutate(std::move(res_column));
    decoder->decode(num_values, *mutable_column, /*filter*/ nullptr, /*filter_offset*/ 0);
    res_column = std::move(mutable_column);
}

size_t MergeTreeReaderParquetSingleBuffer::readRows(
    size_t from_mark, size_t /*current_task_last_mark*/,
    bool continue_reading, size_t max_rows_to_read,
    size_t rows_offset, Columns & res_columns)
{
    if (!initialized)
        init();

    if (continue_reading)
        from_mark = next_mark;

    const size_t total_marks = row_group_first_mark.empty() ? 0 : row_group_first_mark.back();
    const auto & granularity = data_part_info_for_read->getIndexGranularity();

    for (const auto & state : columns)
        if (!res_columns[state.output_idx])
            res_columns[state.output_idx] = getColumnsToRead()[state.output_idx].type->createColumn();

    size_t read_rows = 0;
    while (read_rows < max_rows_to_read && from_mark < total_marks)
    {
        const size_t rows_in_mark = granularity.getMarkRows(from_mark);
        if (rows_in_mark <= rows_offset)
        {
            rows_offset -= rows_in_mark;
            ++from_mark;
            continue;
        }

        for (size_t i = 0; i < columns.size(); ++i)
            readPage(i, from_mark, res_columns[columns[i].output_idx]);

        ++from_mark;
        read_rows += rows_in_mark;
        rows_offset = 0;
    }

    next_mark = from_mark;
    return read_rows;
}

}

#endif
