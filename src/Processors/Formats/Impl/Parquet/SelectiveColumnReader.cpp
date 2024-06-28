#include "SelectiveColumnReader.h"

#include <Columns/ColumnsNumber.h>
#include <bits/stat.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}

Chunk RowGroupChunkReader::readChunk(int rows)
{
    // compute row_set;
    RowSet row_set;
    std::ranges::for_each(filter_columns, [&](auto & column) { reader_columns_mapping[column]->computeRowSet(row_set, rows); });

    // read column
    Columns columns;
    std::ranges::for_each(column_readers, [&](auto & reader) { columns.push_back(reader->read(row_set, rows)); });

    return Chunk(columns, columns[0]->size());
}
void OptionalColumnReader::computeRowSet(RowSet row_set, size_t & rows_to_read)
{
}

void SelectiveColumnReader::readPageIfNeeded()
{
    if (!state.page || !state.remain_rows)
        readPage();
    // may read dict page first;
    if (!state.remain_rows)
        readPage();
}

void SelectiveColumnReader::readPage()
{
    auto page = page_reader->NextPage();
    state.page = page;
    if (page->type() == parquet::PageType::DATA_PAGE)
    {
        readDataPageV1(assert_cast<const parquet::DataPageV1 &>(*page));
    }
    else if (page->type() == parquet::PageType::DICTIONARY_PAGE)
    {
        readDictPage(assert_cast<const parquet::DictionaryPage &>(*page));
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported page type {}", magic_enum::enum_name(page->type()));
    }
}
void SelectiveColumnReader::readDataPageV1(const parquet::DataPageV1 & page)
{
    parquet::LevelDecoder decoder;
    state.buffer = page.data();
    auto max_size = page.size();
    state.remain_rows = page.num_values();
    auto max_rep_level = scan_spec.column_desc->max_repetition_level();
    auto max_def_level = scan_spec.column_desc->max_definition_level();
    if (scan_spec.column_desc->max_repetition_level() > 0)
    {
        auto rep_bytes = decoder.SetData(page.repetition_level_encoding(), max_rep_level, state.remain_rows, state.buffer, max_size);
        max_size -= rep_bytes;
        state.buffer += rep_bytes;
        state.rep_levels.resize_fill(state.remain_rows);
        decoder.Decode(state.remain_rows, state.rep_levels.data());
    }
    if (scan_spec.column_desc->max_definition_level() > 0)
    {
        auto def_bytes = decoder.SetData(page.definition_level_encoding(), max_def_level, state.remain_rows, state.buffer, max_size);
        max_size -= def_bytes;
        state.buffer += def_bytes;
        state.def_levels.resize_fill(state.remain_rows);
        decoder.Decode(state.remain_rows, state.def_levels.data());
    }
}

void Int64ColumnDirectReader::computeRowSet(RowSet row_set, size_t & rows_to_read)
{
    readPageIfNeeded();
    rows_to_read = std::min(state.remain_rows, rows_to_read);
    size_t row_set_offset = row_set.offset();
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    for (size_t i = 0; i < rows_to_read; i++)
    {
        row_set.set(i, scan_spec.filter->testInt64(start[i]));
    }
}

void Int64ColumnDirectReader::read(MutableColumnPtr & column, RowSet row_set, size_t & rows_to_read)
{
    ColumnInt64 * data = assert_cast<ColumnInt64 *>(column.get());
    rows_to_read = std::min(state.remain_rows, rows_to_read);
    size_t rows_readed = 0;
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    while (rows_readed < rows_to_read)
    {
        if (row_set.get(row_set.offset() + rows_readed))
        {
            data->getData().push_back(start[rows_readed]);
        }
        rows_readed ++;
    }
    state.buffer += rows_to_read * sizeof(Int64);
    state.remain_rows -= rows_to_read;
}

}
