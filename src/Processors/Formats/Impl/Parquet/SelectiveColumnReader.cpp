#include "SelectiveColumnReader.h"

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
extern const int PARQUET_EXCEPTION;
}

Chunk RowGroupChunkReader::readChunk(size_t rows)
{
    rows = std::min(rows, remain_rows);
    MutableColumns columns;

    for (auto & reader : column_readers)
    {
        columns.push_back(reader->createColumn());
        columns.back()->reserve(rows);
    }

    size_t rows_read = 0;
    while (rows_read < rows)
    {
        size_t rows_to_read = std::min(rows - rows_read, remain_rows);
        if (!rows_to_read)
            break;
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
            reader_columns_mapping[column]->computeRowSet(row_set, rows_to_read);
        }
        bool skip_all = row_set.none();
        for (size_t i = 0; i < column_readers.size(); i++)
        {
            if (skip_all)
                column_readers[i]->skip(rows_to_read);
            else
                column_readers[i]->read(columns[i], row_set, rows_to_read);
        }
        remain_rows -= rows_to_read;
        rows_read = columns[0]->size();
    }
    return Chunk(std::move(columns), rows_read);
}
RowGroupChunkReader::RowGroupChunkReader(ParquetReader * parquetReader,
                                         std::shared_ptr<parquet::RowGroupReader> rowGroupReader,
                                         std::unordered_map<String, ColumnFilterPtr> filters)
    : parquet_reader(parquetReader), row_group_reader(rowGroupReader)
{
    std::unordered_map<String, parquet::schema::NodePtr> parquet_columns;
    const auto * root = parquet_reader->meta_data->schema()->group_node();
    for (int i = 0; i < root->field_count(); ++i)
    {
        const auto & node = root->field(i);
        parquet_columns.emplace(node->name(), node);
    }

    column_readers.reserve(parquet_reader->header.columns());
    for (const auto & col_with_name : parquet_reader->header)
    {
        if (!parquet_columns.contains(col_with_name.name))
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "no column with '{}' in parquet file", col_with_name.name);

        const auto & node = parquet_columns.at(col_with_name.name);
        if (!node->is_primitive())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "arrays and maps are not implemented in native parquet reader");

        auto idx = parquet_reader->meta_data->schema()->ColumnIndex(*node);
        auto filter = filters.contains(col_with_name.name) ? filters.at(col_with_name.name) : nullptr;
        auto column_reader = SelectiveColumnReaderFactory::createLeafColumnReader(
            *row_group_reader->metadata()->ColumnChunk(idx),
            parquet_reader->meta_data->schema()->Column(idx),
            row_group_reader->GetColumnPageReader(idx),
            filter);
        if (node->is_optional())
        {
            column_reader = SelectiveColumnReaderFactory::createOptionalColumnReader(column_reader, filter);
        }
        column_readers.push_back(column_reader);
        reader_columns_mapping[col_with_name.name] = column_reader;
        chassert(idx >= 0);
        if (filter) filter_columns.push_back(col_with_name.name);
    }
    remain_rows = row_group_reader->metadata()->num_rows();
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
        readDataPageV1(static_cast<const parquet::DataPageV1 &>(*page));
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
        auto rep_bytes = decoder.SetData(page.repetition_level_encoding(), max_rep_level, static_cast<int>(state.remain_rows), state.buffer, max_size);
        max_size -= rep_bytes;
        state.buffer += rep_bytes;
        state.rep_levels.resize_fill(state.remain_rows);
        decoder.Decode(static_cast<int>(state.remain_rows), state.rep_levels.data());
    }
    if (scan_spec.column_desc->max_definition_level() > 0)
    {
        auto def_bytes = decoder.SetData(page.definition_level_encoding(), max_def_level, static_cast<int>(state.remain_rows), state.buffer, max_size);
        state.buffer += def_bytes;
        state.def_levels.resize_fill(state.remain_rows);
        decoder.Decode(static_cast<int>(state.remain_rows), state.def_levels.data());
    }
}

template <typename DataType>
void Int64ColumnDirectReader<DataType>::computeRowSet(RowSet& row_set, size_t rows_to_read)
{
    readPageIfNeeded();
    chassert(rows_to_read <= state.remain_rows);
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    if (scan_spec.filter)
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            bool pass = scan_spec.filter->testInt64(start[i]);
            row_set.set(i, pass);
        }
    }

}

template <typename DataType>
void Int64ColumnDirectReader<DataType>::read(MutableColumnPtr & column, RowSet & row_set, size_t rows_to_read)
{
    auto * int_column = static_cast<DataType::ColumnType *>(column.get());
    auto & data = int_column->getData();
    size_t rows_read = 0;
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    while (rows_read < rows_to_read)
    {
        if (row_set.get(rows_read))
        {
            data.push_back(start[rows_read]);
        }
        rows_read ++;
    }
    state.buffer += rows_to_read * sizeof(Int64);
    state.remain_rows -= rows_to_read;
}

template <typename DataType>
void Int64ColumnDirectReader<DataType>::skip(size_t rows)
{
    state.remain_rows -= rows;
    state.buffer += rows * sizeof(Int64);
}

template <typename DataType>
void Int64ColumnDirectReader<DataType>::readSpace(MutableColumnPtr & column, RowSet & row_set, PaddedPODArray<UInt8>& null_map, size_t rows_to_read)
{
    auto * int_column = static_cast<DataType::ColumnType *>(column.get());
    auto & data = int_column->getData();
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
                count++;
            }
        }
        rows_read ++;
    }
    state.buffer += count * sizeof(Int64);
    state.remain_rows -= rows_to_read;
}

template <typename DataType>
void Int64ColumnDirectReader<DataType>::computeRowSetSpace(RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
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

template <typename DataType>
MutableColumnPtr Int64ColumnDirectReader<DataType>::createColumn()
{
    return DataType::ColumnType::create();
}

template <typename DataType>
Int64ColumnDirectReader<DataType>::Int64ColumnDirectReader(std::unique_ptr<parquet::PageReader> page_reader_, ScanSpec scan_spec_)
    : SelectiveColumnReader(std::move(page_reader_), scan_spec_)
{
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
            if (cur_null_map[i])
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

void OptionalColumnReader::read(MutableColumnPtr & column, RowSet& row_set, size_t rows_to_read)
{
    rows_to_read = std::min(child->currentRemainRows(), rows_to_read);
    auto* nullable_column = static_cast<ColumnNullable *>(column.get());
    auto nested_column = nullable_column->getNestedColumnPtr()->assumeMutable();
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
    cleanNullMap();
}

void OptionalColumnReader::skip(size_t rows)
{
    if (cur_null_map.empty())
        nextBatchNullMap(rows);
    else
        chassert(rows == cur_null_map.size());
    child->skipNulls(cur_null_count);
    child->skip(rows - cur_null_count);
    cleanNullMap();
}

MutableColumnPtr OptionalColumnReader::createColumn()
{
    return ColumnNullable::create(child->createColumn(), ColumnUInt8::create());
}


SelectiveColumnReaderPtr SelectiveColumnReaderFactory::createLeafColumnReader(
    const parquet::ColumnChunkMetaData& column_metadata, const parquet::ColumnDescriptor * column_desc, std::unique_ptr<parquet::PageReader> page_reader, ColumnFilterPtr filter)
{
    ScanSpec scan_spec{.column_name=column_desc->name(), .column_desc=column_desc, .filter=filter};
    if (column_desc->physical_type() == parquet::Type::INT64 &&
        (column_desc->logical_type()->type() == parquet::LogicalType::Type::INT
         || column_desc->logical_type()->type() == parquet::LogicalType::Type::NONE))
    {
        bool plain_encoding = column_metadata.encodings().size() == 1 && column_metadata.encodings()[0] == parquet::Encoding::PLAIN;
        if (plain_encoding)
            return std::make_shared<Int64ColumnDirectReader<DataTypeInt64>>(std::move(page_reader), scan_spec);
        else
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported encoding for int64 column");
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported column type");
    }
}
SelectiveColumnReaderPtr SelectiveColumnReaderFactory::createOptionalColumnReader(SelectiveColumnReaderPtr child, ColumnFilterPtr filter)
{
    ScanSpec scan_spec;
    scan_spec.filter = filter;
    return std::make_shared<OptionalColumnReader>(scan_spec, std::move(child));
}


template class Int64ColumnDirectReader<DataTypeInt64>;
}
