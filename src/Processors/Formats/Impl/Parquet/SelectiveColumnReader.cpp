#include "SelectiveColumnReader.h"

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <bits/stat.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}

Chunk RowGroupChunkReader::readChunk(size_t rows)
{
    Columns columns;

    for (auto & reader : column_readers)
    {
        columns.push_back(reader->createColumn());
        columns.back()->reserve(rows);
    }

    size_t rows_read = 0;
    while (rows_read < rows)
    {
        size_t rows_to_read = rows - rows_read;
        for (auto & reader : column_readers)
        {
            if (!reader->currentRemainRows())
            {
                reader->readPageIfNeeded();
            }
            rows_to_read = std::min(reader->currentRemainRows(), rows_to_read);
        }
        if (!rows_to_read)
            break;

        RowSet row_set(rows_to_read);
        for (auto & column : filter_columns)
        {
            reader_columns_mapping[column]->computeRowSet(row_set, rows);
        }
        bool skip_all = row_set.none();
        for (size_t i = 0; i < column_readers.size(); i++)
        {
            if (skip_all)
                column_readers[i]->skip(rows_to_read);
            else
                column_readers[i]->read(columns[i], row_set, rows_to_read);
        }
        rows_read += columns[0]->size();
    }
    return Chunk(columns, rows_read);
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
    auto max_size = page.size();
    state.remain_rows = page.num_values();
    state.buffer = page.data();
    auto max_rep_level = scan_spec.column_desc->max_repetition_level();
    auto max_def_level = scan_spec.column_desc->max_definition_level();
    state.def_levels.resize(0);
    state.rep_levels.resize(0);
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
        state.buffer += def_bytes;
        state.def_levels.resize_fill(state.remain_rows);
        decoder.Decode(state.remain_rows, state.def_levels.data());
    }
}

void Int64ColumnDirectReader::computeRowSet(RowSet& row_set, size_t offset, size_t value_offset, size_t rows_to_read)
{
    readPageIfNeeded();
    chassert(rows_to_read <= state.remain_rows);
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer) + value_offset;
    if (scan_spec.filter)
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            row_set.set(offset + i, scan_spec.filter->testInt64(start[i]));
        }
    }

}

void Int64ColumnDirectReader::read(MutableColumnPtr & column, RowSet & row_set, size_t rows_to_read)
{
    ColumnInt64 * data = assert_cast<ColumnInt64 *>(column.get());
    size_t rows_read = 0;
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    while (rows_read < rows_to_read)
    {
        if (row_set.get(rows_read))
        {
            data->getData().push_back(start[rows_read]);
        }
        rows_readed ++;
    }
    state.buffer += rows_to_read * sizeof(Int64);
    state.remain_rows -= rows_to_read;
}
void Int64ColumnDirectReader::skip(size_t rows)
{
    state.remain_rows -= rows;
    state.buffer += rows * sizeof(Int64);
}
void Int64ColumnDirectReader::readSpace(MutableColumnPtr & column, RowSet & row_set, PaddedPODArray<UInt8>& null_map, size_t rows_to_read)
{
    auto * column = assert_cast<ColumnInt64 *>(column.get());
    auto & data = column->getData();
    size_t rows_read = 0;
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    size_t count = 0;
    while (rows_read < rows_to_read)
    {
        if (row_set.get(rows_read))
        {
            if (null_map[rows_read])
            {
                column->insertDefault();
            }
            else
            {
                data.push_back(start[count]);
                count++
            }
        }
        rows_read ++;
    }
    state.buffer += count * sizeof(Int64);
    state.remain_rows -= rows_to_read;
}
void Int64ColumnDirectReader::computeRowSetSpace(RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
{
    readPageIfNeeded();
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    if (!scan_spec.filter) return;
    int count = 0;
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (null_map[i])
        {
            row_set.set(i, scan_spec.filter->testNull());
        }
        else
        {
            row_set.set(i, scan_spec.filter->testInt64(start[count]));
            count++;
        }
    }
}
MutableColumnPtr Int64ColumnDirectReader::createColumn()
{
    return ColumnInt64::create();
}

size_t OptionalColumnReader::currentRemainRows() const
{
    return child->currentRemainRows();
}

void OptionalColumnReader::nextBatchNullMap(size_t rows_to_read)
{
    cur_null_map.resize_fill(rows_to_read, 0);
    cur_null_count = 0;
    const auto & def_levels = child->getDefinitionLevels();
    size_t start = def_levels.size() - currentRemainRows();
    int16_t max_def_level = max_definition_level();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (def_levels[start + i] < max_def_level)
        {
            cur_null_map[i] = 1;
            cur_null_count ++;
        }
    }
}

void OptionalColumnReader::computeRowSet(RowSet& row_set, size_t rows_to_read)
{
    nextBatchNullMap(rows_to_read);
    if (scan_spec.filter)
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (null_map[i])
            {
                row_set.set(i, scan_spec.filter->testNull());
            }
            else
            {
                row_set.set(i, scan_spec.filter->testNotNull());
            }
        }
    }
    if (cur_null_count)
        child->computeRowSetSpace(row_set, cur_null_map, rows_to_read);
    else
        child->computeRowSet(row_set, rows_to_read);
}

void OptionalColumnReader::read(MutableColumnPtr & column, RowSet& row_set, size_t , size_t rows_to_read)
{
    rows_to_read = std::min(child->currentRemainRows(), rows_to_read);
    auto* nullable_column = static_cast<ColumnNullable *>(column.get());
    auto & nested_column = nullable_column->getNestedColumn();
    auto & null_data = nullable_column->getNullMapData();

    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (row_set.get(i))
        {
            null_data.push_back(cur_null_map[i]);
        }
    }
    if (cur_null_count)
    {
        child->readSpace(nested_column, row_set, cur_null_map, rows_to_read);
    }
    else
    {
        child->read(nested_column, row_set, rows_to_read);
    }
}

void OptionalColumnReader::skip(size_t rows)
{
    if (cur_null_map.empty())
        nextBatchNullMap(rows);
    else
        chassert(rows == cur_null_map.size());
    child->skipNulls(cur_null_count);
    child->skip(rows - cur_null_count);
}

MutableColumnPtr OptionalColumnReader::createColumn()
{
    return ColumnNullable::create(child->createColumn(), ColumnUInt8::create());
}


}
