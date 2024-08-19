#include "SelectiveColumnReader.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
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
    if (!remain_rows) return {};
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

        OptionalRowSet row_set = std::nullopt;
        if (!filter_columns.empty())
            row_set = std::optional(RowSet(rows_to_read));
        for (auto & column : filter_columns)
        {
            reader_columns_mapping[column]->computeRowSet(row_set, rows_to_read);
            if (row_set.value().none())
                break;
        }
        bool skip_all = false;
        if (row_set.has_value())  skip_all = row_set.value().none();
        if (skip_all)
        {
            metrics.skipped_rows += rows_to_read;
        }

        bool all = false;
        if (row_set.has_value())  all = row_set.value().all();
        if (all) row_set = std::nullopt;

        for (size_t i = 0; i < column_readers.size(); i++)
        {
            if (skip_all)
                column_readers[i]->skip(rows_to_read);
            else
                column_readers[i]->read(columns[i], row_set, rows_to_read);
        }
        remain_rows -= rows_to_read;
        metrics.filtered_rows += (rows_to_read - (columns[0]->size() - rows_read));
        rows_read = columns[0]->size();
    }

    if (parquet_reader->remain_filter.has_value())
    {
        auto input = parquet_reader->header.cloneWithColumns(std::move(columns));
        auto output = input.getColumns();
        parquet_reader->remain_filter.value().execute(input);
        const auto& filter = checkAndGetColumn<ColumnUInt8>(*input.getByPosition(0).column).getData();
        size_t resize_hint = 0;
        for (size_t i = 0; i < columns.size(); i++)
        {
            output[i] = output[i]->assumeMutable()->filter(filter, resize_hint);
            resize_hint = output[i]->size();
        }
        return Chunk(std::move(output), resize_hint);
    }
    metrics.output_rows += rows_read;
    return Chunk(std::move(columns), rows_read);
}

std::pair<size_t, size_t> getColumnRange(const parquet::ColumnChunkMetaData & column_metadata)
{
    size_t col_start = column_metadata.data_page_offset();
    if (column_metadata.has_dictionary_page() && column_metadata.dictionary_page_offset() > 0
        && col_start > static_cast<size_t>(column_metadata.dictionary_page_offset()))
    {
        col_start = column_metadata.dictionary_page_offset();
    }
    size_t len = column_metadata.total_compressed_size();
    return {col_start, len};
}

RowGroupChunkReader::RowGroupChunkReader(
    ParquetReader * parquetReader,
    std::shared_ptr<parquet::RowGroupMetaData> row_group_meta_,
    std::unordered_map<String, ColumnFilterPtr> filters)
    : parquet_reader(parquetReader), row_group_meta(row_group_meta_)
{
    std::unordered_map<String, parquet::schema::NodePtr> parquet_columns;
    const auto * root = parquet_reader->meta_data->schema()->group_node();
    for (int i = 0; i < root->field_count(); ++i)
    {
        const auto & node = root->field(i);
        parquet_columns.emplace(node->name(), node);
    }

    column_readers.reserve(parquet_reader->header.columns());
    column_buffers.resize(parquet_reader->header.columns());
    int reader_idx = 0;
    for (const auto & col_with_name : parquet_reader->header)
    {
        if (!parquet_columns.contains(col_with_name.name))
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "no column with '{}' in parquet file", col_with_name.name);

        const auto & node = parquet_columns.at(col_with_name.name);
        if (!node->is_primitive())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "arrays and maps are not implemented in native parquet reader");

        auto idx = parquet_reader->meta_data->schema()->ColumnIndex(*node);
        auto filter = filters.contains(col_with_name.name) ? filters.at(col_with_name.name) : nullptr;
        auto range = getColumnRange(*row_group_meta->ColumnChunk(idx));
        size_t compress_size = range.second;
        size_t offset = range.first;
        column_buffers[reader_idx].resize(compress_size, 0);
        remain_rows = row_group_meta->ColumnChunk(idx)->num_values();
        parquet_reader->file.seek(offset, SEEK_SET);
        size_t count = parquet_reader->file.readBig(reinterpret_cast<char *>(column_buffers[reader_idx].data()), compress_size);
        if (count != compress_size)
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Failed to read column data");
        auto page_reader = std::make_unique<LazyPageReader>(
            std::make_shared<ReadBufferFromMemory>(reinterpret_cast<char *>(column_buffers[reader_idx].data()), compress_size),
            parquet_reader->properties,
            remain_rows,
            row_group_meta->ColumnChunk(idx)->compression());
        auto column_reader = SelectiveColumnReaderFactory::createLeafColumnReader(
            *row_group_meta->ColumnChunk(idx), parquet_reader->meta_data->schema()->Column(idx), std::move(page_reader), filter);
        if (node->is_optional())
        {
            column_reader = SelectiveColumnReaderFactory::createOptionalColumnReader(column_reader, nullptr);
        }
        column_readers.push_back(column_reader);
        reader_columns_mapping[col_with_name.name] = column_reader;
        chassert(idx >= 0);
        if (filter)
            filter_columns.push_back(col_with_name.name);
        reader_idx++;
    }
}


template <typename T>
void PlainDecoder::decodeFixedValue(PaddedPODArray<T> & data, const OptionalRowSet & row_set, size_t rows_to_read)
{
    const T * start = reinterpret_cast<const T *>(buffer);
    if (!row_set.has_value())
        data.insert_assume_reserved(start, start + rows_to_read);
    else
    {
        const auto & sets = row_set.value();
        FilterHelper::filterPlainFixedData(start, data, sets, rows_to_read);
    }
    buffer += rows_to_read * sizeof(T);
    remain_rows -= rows_to_read;
}

void SelectiveColumnReader::readPageIfNeeded()
{
    skipPageIfNeed();
    while (!state.remain_rows)
    {
        if (!readPage())
            break;
    }
}

bool SelectiveColumnReader::readPage()
{
    if (!page_reader->hasNext())
        return false;
    auto page_header = page_reader->peekNextPageHeader();
    auto page_type = page_header.type;
    if (page_type == parquet::format::PageType::DICTIONARY_PAGE)
    {
        auto dict_page = page_reader->nextPage();
        readDictPage(static_cast<const parquet::DictionaryPage &>(*dict_page));
    }
    else if (page_type == parquet::format::PageType::DATA_PAGE)
    {
        state.remain_rows = page_header.data_page_header.num_values;
        state.page.reset();
        skipPageIfNeed();
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported page type {}", magic_enum::enum_name(page_type));
    }
    return true;
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
        auto rep_bytes
            = decoder.SetData(page.repetition_level_encoding(), max_rep_level, static_cast<int>(state.remain_rows), state.buffer, max_size);
        max_size -= rep_bytes;
        state.buffer += rep_bytes;
        state.rep_levels.resize_fill(state.remain_rows);
        decoder.Decode(static_cast<int>(state.remain_rows), state.rep_levels.data());
    }
    if (scan_spec.column_desc->max_definition_level() > 0)
    {
        auto def_bytes
            = decoder.SetData(page.definition_level_encoding(), max_def_level, static_cast<int>(state.remain_rows), state.buffer, max_size);
        max_size -= def_bytes;
        state.buffer += def_bytes;
        state.def_levels.resize_fill(state.remain_rows);
        decoder.Decode(static_cast<int>(state.remain_rows), state.def_levels.data());
    }
    state.buffer_size = max_size;
    if (page.encoding() == parquet::Encoding::RLE_DICTIONARY || page.encoding() == parquet::Encoding::PLAIN_DICTIONARY)
    {
        initIndexDecoderIfNeeded();
        createDictDecoder();
        plain = false;
    }
    else if (page.encoding() == parquet::Encoding::PLAIN)
    {
        if (!plain)
        {
            downgradeToPlain();
            plain = true;
        }
        plain_decoder = std::make_unique<PlainDecoder>(state.buffer, state.remain_rows);
    }
    else
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported encoding type {}", magic_enum::enum_name(page.encoding()));
    }
    state.lazy_skip_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
    chassert(state.lazy_skip_rows == 0);
}
void SelectiveColumnReader::decodePage()
{
    if (state.page)
        return;
    state.page = page_reader->nextPage();
    readDataPageV1(static_cast<const parquet::DataPageV1 &>(*state.page));
}
void SelectiveColumnReader::skipPageIfNeed()
{
    if (!state.page && state.remain_rows && state.remain_rows <= state.lazy_skip_rows)
    {
        // skip page
        state.lazy_skip_rows -= state.remain_rows;
        page_reader->skipNextPage();
        //        std::cerr << "skip page :" << state.remain_rows << std::endl;
        state.remain_rows = 0;
    }
}
void SelectiveColumnReader::skip(size_t rows)
{
    state.lazy_skip_rows += rows;
    state.lazy_skip_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
    skipPageIfNeed();
}
void SelectiveColumnReader::skipNulls(size_t rows_to_skip)
{
    auto skipped = std::min(rows_to_skip, state.remain_rows);
    state.remain_rows -= skipped;
    state.lazy_skip_rows += (rows_to_skip - skipped);
}

template <typename T>
void computeRowSetPlain(const T * start, OptionalRowSet & row_set, const ColumnFilterPtr & filter, size_t rows_to_read)
{
    if (filter)
    {
        if constexpr (std::is_same_v<T, Int64>)
        {
            if (row_set.has_value())
                filter->testInt64Values(row_set.value(), 0, rows_to_read, start);
        }
        else
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported type");
    }
}

template <typename DataType>
void NumberColumnDirectReader<DataType>::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    chassert(rows_to_read <= state.remain_rows);
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    computeRowSetPlain(start, row_set, scan_spec.filter, rows_to_read);
}

template <typename DataType>
void NumberColumnDirectReader<DataType>::read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    auto * int_column = static_cast<DataType::ColumnType *>(column.get());
    auto & data = int_column->getData();
    plain_decoder->decodeFixedValue(data, row_set, rows_to_read);
}

template <typename DataType>
size_t NumberColumnDirectReader<DataType>::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.remain_rows, rows_to_skip);
    state.remain_rows -= skipped;
    state.buffer += skipped * sizeof(Int64);
    return rows_to_skip - skipped;
}

template <typename DataType>
void NumberColumnDirectReader<DataType>::readSpace(
    MutableColumnPtr & column, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    readAndDecodePage();
    auto * int_column = static_cast<DataType::ColumnType *>(column.get());
    auto & data = int_column->getData();
    plain_decoder->decodeFixedValueSpace(data, row_set, null_map, rows_to_read);
}

template <typename T>
void computeRowSetPlainSpace(
    const T * start, OptionalRowSet & row_set, const ColumnFilterPtr & filter, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
{
    if (!filter || !row_set.has_value())
        return;
    int count = 0;
    auto & sets = row_set.value();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (null_map[i])
        {
            sets.set(i, filter->testNull());
        }
        else
        {
            sets.set(i, filter->testInt64(start[count]));
            count++;
        }
    }
}

template <typename DataType>
void NumberColumnDirectReader<DataType>::computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    readAndDecodePage();
    const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
    computeRowSetPlainSpace(start, row_set, scan_spec.filter, null_map, rows_to_read);
}

template <typename DataType>
MutableColumnPtr NumberColumnDirectReader<DataType>::createColumn()
{
    return DataType::ColumnType::create();
}

template <typename DataType>
NumberColumnDirectReader<DataType>::NumberColumnDirectReader(std::unique_ptr<LazyPageReader> page_reader_, ScanSpec scan_spec_)
    : SelectiveColumnReader(std::move(page_reader_), scan_spec_)
{
}

template <typename DataType>
NumberDictionaryReader<DataType>::NumberDictionaryReader(std::unique_ptr<LazyPageReader> page_reader_, ScanSpec scan_spec_)
    : SelectiveColumnReader(std::move(page_reader_), scan_spec_)
{
}

template <typename DataType>
void NumberDictionaryReader<DataType>::nextIdxBatchIfEmpty(size_t rows_to_read)
{
    if (!state.idx_buffer.empty() || plain)
        return;
    state.idx_buffer.resize(rows_to_read);
    idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(rows_to_read));
}

template <typename DataType>
void NumberDictionaryReader<DataType>::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    chassert(rows_to_read <= state.remain_rows);
    if (plain)
    {
        const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
        computeRowSetPlain(start, row_set, scan_spec.filter, rows_to_read);
        return;
    }
    nextIdxBatchIfEmpty(rows_to_read);
    if (scan_spec.filter || row_set.has_value())
    {
        auto & cache = *state.filter_cache;
        auto & sets = row_set.value();
        for (size_t i = 0; i < rows_to_read; ++i)
        {
            int idx = state.idx_buffer[i];
            if (!cache.has(idx))
            {
                if constexpr (std::is_same_v<typename DataType::FieldType, Int64>)
                    cache.set(idx, scan_spec.filter->testInt64(dict[idx]));
                else if constexpr (std::is_same_v<typename DataType::FieldType, Float32>)
                    cache.set(idx, scan_spec.filter->testFloat32(dict[idx]));
                else if constexpr (std::is_same_v<typename DataType::FieldType, Float64>)
                    cache.set(idx, scan_spec.filter->testFloat64(dict[idx]));
                else
                    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported type");
            }
            sets.set(i, cache.get(idx));
        }
    }
}

template <typename DataType>
void NumberDictionaryReader<DataType>::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    readAndDecodePage();
    chassert(rows_to_read <= state.remain_rows);
    if (plain)
    {
        const Int64 * start = reinterpret_cast<const Int64 *>(state.buffer);
        computeRowSetPlainSpace(start, row_set, scan_spec.filter, null_map, rows_to_read);
        return;
    }
    auto nonnull_count = rows_to_read - null_count;
    nextIdxBatchIfEmpty(nonnull_count);
    if (scan_spec.filter || row_set.has_value())
    {
        auto & cache = *state.filter_cache;
        int count = 0;
        auto & sets = row_set.value();
        for (size_t i = 0; i < rows_to_read; ++i)
        {
            if (null_map[i])
            {
                if (!cache.hasNull())
                {
                    cache.setNull(scan_spec.filter->testNull());
                }
                sets.set(i, cache.getNull());
            }
            else
            {
                int idx = state.idx_buffer[count++];
                if (!cache.has(idx))
                {
                    if constexpr (std::is_same_v<typename DataType::FieldType, Int64>)
                        cache.set(idx, scan_spec.filter->testInt64(dict[idx]));
                    else
                        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported type");
                }
                sets.set(i, cache.get(idx));
            }
        }
    }
}

template <typename DataType>
void NumberDictionaryReader<DataType>::read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    auto * int_column = static_cast<DataType::ColumnType *>(column.get());
    auto & data = int_column->getData();
    nextIdxBatchIfEmpty(rows_to_read);
    if (plain)
        plain_decoder->decodeFixedValue(data, row_set, rows_to_read);
    else
    {
        dict_decoder->decodeFixedValue(dict, data, row_set, rows_to_read);
    }
}

template <typename DataType>
void NumberDictionaryReader<DataType>::readSpace(
    MutableColumnPtr & column, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    readAndDecodePage();
    auto * int_column = static_cast<DataType::ColumnType *>(column.get());
    auto & data = int_column->getData();
    nextIdxBatchIfEmpty(rows_to_read - null_count);
    if (plain)
        plain_decoder->decodeFixedValueSpace(data, row_set, null_map, rows_to_read);
    else
        dict_decoder->decodeFixedValueSpace(dict, data, row_set, null_map, rows_to_read);
}

template <typename DataType>
void NumberDictionaryReader<DataType>::readDictPage(const parquet::DictionaryPage & page)
{
    const auto * dict_data = page.data();
    auto dict_size = page.num_values();
    dict.resize(dict_size);
    state.filter_cache = std::make_unique<FilterCache>(dict_size);
    memcpy(dict.data(), dict_data, dict_size * sizeof(typename DataType::FieldType));
}

template <typename DataType>
size_t NumberDictionaryReader<DataType>::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.remain_rows, rows_to_skip);
    state.remain_rows -= skipped;
    if (plain)
    {
        state.buffer += skipped * sizeof(Int64);
    }
    else
    {
        if (!state.idx_buffer.empty())
        {
            // only support skip all
            chassert(state.idx_buffer.size() == skipped);
            state.idx_buffer.resize(0);
        }
        else
        {
            state.idx_buffer.resize(skipped);
            idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(skipped));
            state.idx_buffer.resize(0);
        }
    }
    return rows_to_skip - skipped;
}

template <typename DataType>
void NumberDictionaryReader<DataType>::createDictDecoder()
{
    dict_decoder = std::make_unique<DictDecoder>(state.idx_buffer, state.remain_rows);
}

template <typename DataType>
void NumberDictionaryReader<DataType>::downgradeToPlain()
{
    dict.resize(0);
    dict_decoder.reset();
}


size_t OptionalColumnReader::currentRemainRows() const
{
    return child->currentRemainRows();
}

void OptionalColumnReader::nextBatchNullMapIfNeeded(size_t rows_to_read)
{
    if (!cur_null_map.empty())
        return;
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
            cur_null_count++;
        }
    }
}

void OptionalColumnReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    applyLazySkip();
    nextBatchNullMapIfNeeded(rows_to_read);
    if (cur_null_count)
        child->computeRowSetSpace(row_set, cur_null_map, cur_null_count, rows_to_read);
    else
        child->computeRowSet(row_set, rows_to_read);
}

void OptionalColumnReader::read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read)
{
    applyLazySkip();
    nextBatchNullMapIfNeeded(rows_to_read);
    rows_to_read = std::min(child->currentRemainRows(), rows_to_read);
    auto * nullable_column = static_cast<ColumnNullable *>(column.get());
    auto nested_column = nullable_column->getNestedColumnPtr()->assumeMutable();
    auto & null_data = nullable_column->getNullMapData();
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (sets.get(i))
            {
                null_data.push_back(cur_null_map[i]);
            }
        }
    }
    else
        null_data.insert(cur_null_map.begin(), cur_null_map.end());
    if (cur_null_count)
    {
        child->readSpace(nested_column, row_set, cur_null_map, cur_null_count, rows_to_read);
    }
    else
    {
        child->read(nested_column, row_set, rows_to_read);
    }
    cleanNullMap();
}

size_t OptionalColumnReader::skipValuesInCurrentPage(size_t rows)
{
    if (!rows)
        return 0;
    if (!child->currentRemainRows() || !child->state.page)
        return rows;
    auto skipped = std::min(rows, child->currentRemainRows());
    if (cur_null_map.empty())
        nextBatchNullMapIfNeeded(skipped);
    else
        chassert(rows == cur_null_map.size());
    child->skipNulls(cur_null_count);
    child->skip(skipped - cur_null_count);
    cleanNullMap();
    return rows - skipped;
}

MutableColumnPtr OptionalColumnReader::createColumn()
{
    return ColumnNullable::create(child->createColumn(), ColumnUInt8::create());
}
void OptionalColumnReader::applyLazySkip()
{
    skipPageIfNeed();
    child->readAndDecodePage();
    state.lazy_skip_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
    chassert(!state.lazy_skip_rows);
}
void OptionalColumnReader::skipPageIfNeed()
{
    child->state.lazy_skip_rows = state.lazy_skip_rows;
    child->skipPageIfNeed();
    state.lazy_skip_rows = child->state.lazy_skip_rows;
    child->state.lazy_skip_rows = 0;
}

bool isLogicalTypeIntOrNull(parquet::LogicalType::Type::type type)
{
    return type == parquet::LogicalType::Type::INT || type == parquet::LogicalType::Type::NONE;
}


SelectiveColumnReaderPtr SelectiveColumnReaderFactory::createLeafColumnReader(
    const parquet::ColumnChunkMetaData & column_metadata,
    const parquet::ColumnDescriptor * column_desc,
    std::unique_ptr<LazyPageReader> page_reader,
    ColumnFilterPtr filter)
{
    ScanSpec scan_spec{.column_name = column_desc->name(), .column_desc = column_desc, .filter = filter};
    if (column_desc->physical_type() == parquet::Type::INT64 && isLogicalTypeIntOrNull(column_desc->logical_type()->type()))
    {
        if (!column_metadata.has_dictionary_page())
            return std::make_shared<NumberColumnDirectReader<DataTypeInt64>>(std::move(page_reader), scan_spec);
        else
            return std::make_shared<NumberDictionaryReader<DataTypeInt64>>(std::move(page_reader), scan_spec);
    }
    if (column_desc->physical_type() == parquet::Type::INT32 && isLogicalTypeIntOrNull(column_desc->logical_type()->type()))
    {
        if (!column_metadata.has_dictionary_page())
            return std::make_shared<NumberColumnDirectReader<DataTypeInt32>>(std::move(page_reader), scan_spec);
        else
            return std::make_shared<NumberDictionaryReader<DataTypeInt32>>(std::move(page_reader), scan_spec);
    }
    else if (column_desc->physical_type() == parquet::Type::FLOAT)
    {
        if (!column_metadata.has_dictionary_page())
            return std::make_shared<NumberColumnDirectReader<DataTypeFloat32>>(std::move(page_reader), scan_spec);
        else
            return std::make_shared<NumberDictionaryReader<DataTypeFloat32>>(std::move(page_reader), scan_spec);
    }
    else if (column_desc->physical_type() == parquet::Type::DOUBLE)
    {
        if (!column_metadata.has_dictionary_page())
            return std::make_shared<NumberColumnDirectReader<DataTypeFloat64>>(std::move(page_reader), scan_spec);
        else
            return std::make_shared<NumberDictionaryReader<DataTypeFloat64>>(std::move(page_reader), scan_spec);
    }
    else if (column_desc->physical_type() == parquet::Type::BYTE_ARRAY)
    {
        if (!column_metadata.has_dictionary_page())
            return std::make_shared<StringDirectReader>(std::move(page_reader), scan_spec);
        else
            return std::make_shared<StringDictionaryReader>(std::move(page_reader), scan_spec);
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

template class NumberColumnDirectReader<DataTypeInt32>;
template class NumberColumnDirectReader<DataTypeInt64>;
template class NumberColumnDirectReader<DataTypeFloat32>;
template class NumberColumnDirectReader<DataTypeFloat64>;

template class NumberDictionaryReader<DataTypeInt32>;
template class NumberDictionaryReader<DataTypeInt64>;
template class NumberDictionaryReader<DataTypeFloat32>;
template class NumberDictionaryReader<DataTypeFloat64>;

Int32 loadLength(const uint8_t * data)
{
    auto value_len = arrow::util::SafeLoadAs<Int32>(data);
    if (unlikely(value_len < 0 || value_len > INT32_MAX - 4))
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Invalid or corrupted value_len '{}'", value_len);
    }
    return value_len;
}
void computeRowSetPlainString(const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read)
{
    if (!filter || !row_set.has_value())
        return;
    size_t offset = 0;
    auto & sets = row_set.value();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        auto len = loadLength(start + offset);
        offset += 4;
        sets.set(i, filter->testString(String(reinterpret_cast<const char *>(start + offset), len)));
        offset += len;
    }
}
void computeRowSetPlainStringSpace(
    const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read, PaddedPODArray<UInt8, 4096> & null_map)
{
    if (!filter || !row_set.has_value())
        return;
    size_t offset = 0;
    auto & sets = row_set.value();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (null_map[i])
        {
            sets.set(i, filter->testNull());
            continue;
        }
        auto len = loadLength(start + offset);
        offset += 4;
        sets.set(i, filter->testString(String(reinterpret_cast<const char *>(start + offset), len)));
        offset += len;
    }
}

void StringDictionaryReader::nextIdxBatchIfEmpty(size_t rows_to_read)
{
    if (!state.idx_buffer.empty() || plain)
        return;
    state.idx_buffer.resize(rows_to_read);
    idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(rows_to_read));
}

void StringDictionaryReader::initIndexDecoderIfNeeded()
{
    if (dict.empty())
        return;
    uint8_t bit_width = *state.buffer;
    idx_decoder = arrow::util::RleDecoder(++state.buffer, static_cast<int>(--state.buffer_size), bit_width);
}

void StringDictionaryReader::readDictPage(const parquet::DictionaryPage & page)
{
    const auto * dict_data = page.data();
    size_t dict_size = page.num_values();
    dict.reserve(dict_size);
    state.filter_cache = std::make_unique<FilterCache>(dict_size);
    for (size_t i = 0; i < dict_size; i++)
    {
        auto len = loadLength(dict_data);
        dict_data += 4;
        String value;
        value.resize(len);
        memcpy(value.data(), dict_data, len);
        dict.emplace_back(value);
        dict_data += len;
    }
}

size_t StringDictionaryReader::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.remain_rows, rows_to_skip);
    state.remain_rows -= skipped;
    if (plain)
    {
        size_t offset = 0;
        for (size_t i = 0; i < skipped; i++)
        {
            auto len = loadLength(state.buffer + offset);
            offset += 4 + len;
        }
        state.buffer += offset;
    }
    else
    {
        if (!state.idx_buffer.empty())
        {
            // only support skip all
            chassert(state.idx_buffer.size() == skipped);
            state.idx_buffer.resize(0);
        }
        else
        {
            state.idx_buffer.resize(skipped);
            idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(skipped));
            state.idx_buffer.resize(0);
        }
    }
    return rows_to_skip - skipped;
}

void StringDictionaryReader::readSpace(
    MutableColumnPtr & column, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    readAndDecodePage();
    ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
    if (plain)
    {
        size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.buffer, row_set, null_map, rows_to_read);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
    }
    else
    {
        auto nonnull_count = rows_to_read - null_count;
        nextIdxBatchIfEmpty(nonnull_count);
        dict_decoder->decodeStringSpace(dict, string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
    }
}

void StringDictionaryReader::read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
    if (plain)
    {
        size_t total_size = plain_decoder->calculateStringTotalSize(state.buffer, row_set, rows_to_read);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_to_read);
    }
    else
    {
        nextIdxBatchIfEmpty(rows_to_read);
        dict_decoder->decodeString(dict, string_column->getChars(), string_column->getOffsets(), row_set, rows_to_read);
    }
}

void StringDictionaryReader::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    readAndDecodePage();
    chassert(rows_to_read <= state.remain_rows);
    if (plain)
    {
        computeRowSetPlainStringSpace(state.buffer, row_set, scan_spec.filter, rows_to_read, null_map);
        return;
    }
    auto nonnull_count = rows_to_read - null_count;
    nextIdxBatchIfEmpty(nonnull_count);
    if (scan_spec.filter || row_set.has_value())
    {
        auto & cache = *state.filter_cache;
        auto & sets = row_set.value();
        int count = 0;
        for (size_t i = 0; i < rows_to_read; ++i)
        {
            if (null_map[i])
            {
                if (!cache.hasNull())
                {
                    cache.setNull(scan_spec.filter->testNull());
                }
                sets.set(i, cache.getNull());
            }
            else
            {
                int idx = state.idx_buffer[count++];
                if (!cache.has(idx))
                {
                    cache.set(idx, scan_spec.filter->testString(dict[idx]));
                }
                sets.set(i, cache.get(idx));
            }
        }
    }
}

void StringDictionaryReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    chassert(rows_to_read <= state.remain_rows);
    if (plain)
    {
        computeRowSetPlainString(state.buffer, row_set, scan_spec.filter, rows_to_read);
        return;
    }
    nextIdxBatchIfEmpty(rows_to_read);
    if (scan_spec.filter || row_set.has_value())
    {
        auto & cache = *state.filter_cache;
        for (size_t i = 0; i < rows_to_read; ++i)
        {
            auto & sets = row_set.value();
            int idx = state.idx_buffer[i];
            if (!cache.has(idx))
            {
                cache.set(idx, scan_spec.filter->testString(dict[idx]));
            }
            sets.set(i, cache.get(idx));
        }
    }
}

void StringDictionaryReader::downgradeToPlain()
{
    dict_decoder = nullptr;
    dict.clear();
}

size_t StringDirectReader::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.remain_rows, rows_to_skip);
    state.remain_rows -= skipped;
    size_t offset = 0;
    for (size_t i = 0; i < skipped; i++)
    {
        auto len = loadLength(state.buffer + offset);
        offset += 4 + len;
    }
    state.buffer += offset;
    return rows_to_skip - skipped;
}

void StringDirectReader::readSpace(
    MutableColumnPtr & column, const OptionalRowSet & row_set, PaddedPODArray<UInt8, 4096> & null_map, size_t null_count, size_t rows_to_read)
{
    ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
    size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.buffer, row_set, null_map, rows_to_read - null_count);
    string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
    string_column->getChars().reserve(string_column->getChars().size() + total_size);
    plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
}

void StringDirectReader::read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read)
{
    ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
    size_t total_size = plain_decoder->calculateStringTotalSize(state.buffer, row_set, rows_to_read);
    string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
    string_column->getChars().reserve(string_column->getChars().size() + total_size);
    plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_to_read);
}

void StringDirectReader::computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    readAndDecodePage();
    computeRowSetPlainStringSpace(state.buffer, row_set, scan_spec.filter, rows_to_read, null_map);
}

void StringDirectReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    computeRowSetPlainString(state.buffer, row_set, scan_spec.filter, rows_to_read);
}

void DictDecoder::decodeStringSpace(
    std::vector<String> & dict,
    ColumnString::Chars & chars,
    IColumn::Offsets & offsets,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    size_t rows_to_read)
{
    size_t rows_read = 0;
    size_t count = 0;
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        while (rows_read < rows_to_read)
        {
            if (sets.get(rows_read))
            {
                if (null_map[rows_read])
                {
                    chars.push_back(0);
                    offsets.push_back(chars.size());
                }
                else
                {
                    String value = dict[idx_buffer[count]];
                    chars.insert(value.data(), value.data() + value.size());
                    chars.push_back(0);
                    offsets.push_back(chars.size());
                    count++;
                }
            }
            else if (!null_map[rows_read])
                count++;
            rows_read++;
        }
    }
    else
    {
        while (rows_read < rows_to_read)
        {
            if (null_map[rows_read])
            {
                chars.push_back(0);
                offsets.push_back(chars.size());
            }
            else
            {
                String value = dict[idx_buffer[count]];
                chars.insert(value.data(), value.data() + value.size());
                chars.push_back(0);
                offsets.push_back(chars.size());
                count++;
            }
            rows_read++;
        }
    }
    chassert(count == idx_buffer.size());
    remain_rows -= rows_to_read;
    idx_buffer.resize(0);
}
void DictDecoder::decodeString(
    std::vector<String> & dict,
    ColumnString::Chars & chars,
    IColumn::Offsets & offsets,
    const OptionalRowSet & row_set,
    size_t rows_to_read)
{
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (sets.get(i))
            {
                String & value = dict[idx_buffer[i]];
                auto chars_cursor = chars.size();
                chars.resize(chars_cursor + value.size() + 1);
                memcpySmallAllowReadWriteOverflow15(&chars[chars_cursor], value.data(), value.size());
                chars.back() = 0;
                offsets.push_back(chars.size());
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            String & value = dict[idx_buffer[i]];
            auto chars_cursor = chars.size();
            chars.resize(chars_cursor + value.size() + 1);
            memcpySmallAllowReadWriteOverflow15(&chars[chars_cursor], value.data(), value.size());
            chars.back() = 0;
            offsets.push_back(chars.size());
        }
    }
    idx_buffer.resize(0);
    remain_rows -= rows_to_read;
}
template <class DictValueType>
void DictDecoder::decodeFixedValue(
    PaddedPODArray<DictValueType> & dict,
    PaddedPODArray<DictValueType> & data,
    const OptionalRowSet & row_set,
    size_t rows_to_read)
{
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        FilterHelper::filterDictFixedData(dict, data, idx_buffer, sets, rows_to_read);
    }
    else
    {
        FilterHelper::gatherDictFixedValue(dict, data, idx_buffer, rows_to_read);
    }
    idx_buffer.resize(0);
    remain_rows -= rows_to_read;
}
template <class DictValueType>
void DictDecoder::decodeFixedValueSpace(
    PaddedPODArray<DictValueType> & dict,
    PaddedPODArray<DictValueType> & data,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    size_t rows_to_read)
{
    size_t rows_read = 0;
    size_t count = 0;
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        while (rows_read < rows_to_read)
        {
            if (sets.get(rows_read))
            {
                if (null_map[rows_read])
                    data.push_back(0);
                else
                    data.push_back(dict[idx_buffer[count++]]);
            }
            else if (!null_map[rows_read])
                count++;
            rows_read++;
        }
    }
    else
    {
        while (rows_read < rows_to_read)
        {
            if (null_map[rows_read])
                data.push_back(0);
            else
                data.push_back(dict[idx_buffer[count++]]);
            rows_read++;
        }
    }
    chassert(count == idx_buffer.size());
    remain_rows -= rows_to_read;
    idx_buffer.resize(0);
}
}
