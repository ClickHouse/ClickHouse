#include "SelectiveColumnReader.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeTuple.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
extern const int PARQUET_EXCEPTION;
}

template <typename T, typename S>
static void decodeFixedValueInternal(PaddedPODArray<T> & data, const S * start, const OptionalRowSet & row_set, size_t rows_to_read)
{
    if (!row_set.has_value())
    {
        if constexpr (std::is_same_v<T, S>)
            data.insert_assume_reserved(start, start + rows_to_read);
        else
        {
            auto old_size = data.size();
            data.resize(old_size + rows_to_read);
            for (size_t i = 0; i < rows_to_read; i++)
                data[old_size + i] = static_cast<T>(start[i]);
        }
    }
    else
    {
        const auto & sets = row_set.value();
        FilterHelper::filterPlainFixedData(start, data, sets, rows_to_read);
    }
}

template <typename T, typename S>
void PlainDecoder::decodeFixedValue(PaddedPODArray<T> & data, const OptionalRowSet & row_set, size_t rows_to_read)
{
    const S * start = reinterpret_cast<const S *>(page_data.buffer);
    page_data.checkSize(rows_to_read * sizeof(S));
    decodeFixedValueInternal(data, start, row_set, rows_to_read);
    page_data.consume(rows_to_read * sizeof(S));
    offsets.consume(rows_to_read);
}

void SelectiveColumnReader::readPageIfNeeded()
{
    initPageReaderIfNeed();
    skipPageIfNeed();
    while (!state.offsets.remain_rows)
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
        const parquet::DictionaryPage & dict_page1 = *std::static_pointer_cast<parquet::DictionaryPage>(dict_page);
        if (unlikely(dict_page1.encoding() != parquet::Encoding::PLAIN_DICTIONARY && dict_page1.encoding() != parquet::Encoding::PLAIN))
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported dictionary page encoding {}", dict_page1.encoding());
        }
        readDictPage(static_cast<const parquet::DictionaryPage &>(*dict_page));
    }
    else if (page_type == parquet::format::PageType::DATA_PAGE)
    {
        state.offsets.remain_rows = page_header.data_page_header.num_values;
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
    state.offsets.remain_rows = page.num_values();
    state.data.buffer = page.data();
    auto max_rep_level = scan_spec.column_desc->max_repetition_level();
    auto max_def_level = scan_spec.column_desc->max_definition_level();
    state.def_levels.resize(0);
    state.rep_levels.resize(0);
    if (scan_spec.column_desc->max_repetition_level() > 0)
    {
        auto rep_bytes = decoder.SetData(
            page.repetition_level_encoding(), max_rep_level, static_cast<int>(state.offsets.remain_rows), state.data.buffer, max_size);
        max_size -= rep_bytes;
        state.data.buffer += rep_bytes;
        state.rep_levels.resize_fill(state.offsets.remain_rows);
        decoder.Decode(static_cast<int>(state.offsets.remain_rows), state.rep_levels.data());
    }
    if (scan_spec.column_desc->max_definition_level() > 0)
    {
        auto def_bytes = decoder.SetData(
            page.definition_level_encoding(), max_def_level, static_cast<int>(state.offsets.remain_rows), state.data.buffer, max_size);
        max_size -= def_bytes;
        state.data.buffer += def_bytes;
        state.def_levels.resize_fill(state.offsets.remain_rows);
        decoder.Decode(static_cast<int>(state.offsets.remain_rows), state.def_levels.data());
    }
    state.data.buffer_size = max_size;
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
        plain_decoder = std::make_unique<PlainDecoder>(state.data, state.offsets);
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
    if (!state.page && state.offsets.remain_rows && state.offsets.remain_rows <= state.lazy_skip_rows)
    {
        // skip page
        state.lazy_skip_rows -= state.offsets.remain_rows;
        page_reader->skipNextPage();
        state.offsets.consume(state.offsets.remain_rows);
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
    auto skipped = std::min(rows_to_skip, state.offsets.remain_rows);
    state.offsets.consume(skipped);
    state.lazy_skip_rows += (rows_to_skip - skipped);
}

template <typename T>
static void computeRowSetPlain(const T * start, OptionalRowSet & row_set, const ColumnFilterPtr & filter, size_t rows_to_read)
{
    if (filter && row_set.has_value())
    {
        if constexpr (std::is_same_v<T, Int64>)
            filter->testInt64Values(row_set.value(), rows_to_read, start);
        else if constexpr (std::is_same_v<T, Int32>)
            filter->testInt32Values(row_set.value(), rows_to_read, start);
        else if constexpr (std::is_same_v<T, Int16>)
            filter->testInt16Values(row_set.value(), rows_to_read, start);
        else if constexpr (std::is_same_v<T, Float32>)
            filter->testFloat32Values(row_set.value(), rows_to_read, start);
        else if constexpr (std::is_same_v<T, Float64>)
            filter->testFloat64Values(row_set.value(), rows_to_read, start);
        else
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported type");
    }
}

template <typename DataType, typename SerializedType>
void NumberColumnDirectReader<DataType, SerializedType>::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    chassert(rows_to_read <= state.offsets.remain_rows);
    state.data.checkSize(sizeof(SerializedType) * rows_to_read);
    const SerializedType * start = reinterpret_cast<const SerializedType *>(state.data.buffer);
    computeRowSetPlain(start, row_set, scan_spec.filter, rows_to_read);
}

template <typename DataType, typename SerializedType>
void NumberColumnDirectReader<DataType, SerializedType>::read(
    MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        readAndDecodePage();
        auto * number_column = static_cast<DataType::ColumnType *>(column.get());
        auto & data = number_column->getData();
        if (row_set)
            row_set.value().setOffset(rows_read);
        plain_decoder->decodeFixedValue<typename DataType::FieldType, SerializedType>(data, row_set, rows_can_read);
        rows_read += rows_can_read;
    }
}

template <typename DataType, typename SerializedType>
size_t NumberColumnDirectReader<DataType, SerializedType>::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);
    state.data.consume(skipped * sizeof(SerializedType));
    return rows_to_skip - skipped;
}

template <typename DataType, typename SerializedType>
void NumberColumnDirectReader<DataType, SerializedType>::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        readAndDecodePage();
        auto * number_column = static_cast<DataType::ColumnType *>(column.get());
        auto & data = number_column->getData();
        if (row_set)
            row_set.value().setOffset(rows_read);
        plain_decoder->decodeFixedValueSpace<typename DataType::FieldType, SerializedType>(data, row_set, null_map, rows_can_read);
        rows_read += rows_can_read;
    }
}

template <typename T>
static void computeRowSetPlainSpace(
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
            if constexpr (std::is_same_v<T, Int64>)
                sets.set(i, filter->testInt64(start[count]));
            else if constexpr (std::is_same_v<T, Int32>)
                sets.set(i, filter->testInt32(start[count]));
            else if constexpr (std::is_same_v<T, Int16>)
                sets.set(i, filter->testInt16(start[count]));
            else if constexpr (std::is_same_v<T, Float32>)
                sets.set(i, filter->testFloat32(start[count]));
            else if constexpr (std::is_same_v<T, Float64>)
                sets.set(i, filter->testFloat64(start[count]));
            else
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported type");
            count++;
        }
    }
}

template <typename DataType, typename SerializedType>
void NumberColumnDirectReader<DataType, SerializedType>::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    readAndDecodePage();
    const SerializedType * start = reinterpret_cast<const SerializedType *>(state.data.buffer);
    computeRowSetPlainSpace(start, row_set, scan_spec.filter, null_map, rows_to_read);
}

template <typename DataType, typename SerializedType>
MutableColumnPtr NumberColumnDirectReader<DataType, SerializedType>::createColumn()
{
    return datatype->createColumn();
}

template <typename DataType, typename SerializedType>
NumberColumnDirectReader<DataType, SerializedType>::NumberColumnDirectReader(
    PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_)
    : SelectiveColumnReader(std::move(page_reader_creator_), scan_spec_), datatype(datatype_)
{
}

template <typename DataType, typename SerializedType>
NumberDictionaryReader<DataType, SerializedType>::NumberDictionaryReader(
    PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_)
    : SelectiveColumnReader(std::move(page_reader_creator_), scan_spec_), datatype(datatype_)
{
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::nextIdxBatchIfEmpty(size_t rows_to_read)
{
    if (!batch_buffer.empty() || plain)
        return;
    batch_buffer.resize(rows_to_read);
    size_t count
        = idx_decoder.GetBatchWithDict(dict.data(), static_cast<Int32>(dict.size()), batch_buffer.data(), static_cast<int>(rows_to_read));
    if (count != rows_to_read)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "read full idx batch failed. read {} rows, expect {}", count, rows_to_read);
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    if (!scan_spec.filter || !row_set.has_value())
        return;
    readAndDecodePage();
    chassert(rows_to_read <= state.offsets.remain_rows);
    if (plain)
    {
        const SerializedType * start = reinterpret_cast<const SerializedType *>(state.data.buffer);
        state.data.checkSize(rows_to_read * sizeof(SerializedType));
        computeRowSetPlain(start, row_set, scan_spec.filter, rows_to_read);
        return;
    }
    nextIdxBatchIfEmpty(rows_to_read);
    computeRowSetPlain(batch_buffer.data(), row_set, scan_spec.filter, rows_to_read);
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    if (!scan_spec.filter || !row_set.has_value())
        return;
    readAndDecodePage();
    chassert(rows_to_read <= state.offsets.remain_rows);
    if (plain)
    {
        const SerializedType * start = reinterpret_cast<const SerializedType *>(state.data.buffer);
        computeRowSetPlainSpace(start, row_set, scan_spec.filter, null_map, rows_to_read);
        return;
    }
    auto nonnull_count = rows_to_read - null_count;
    nextIdxBatchIfEmpty(nonnull_count);

    int count = 0;
    auto & sets = row_set.value();
    for (size_t i = 0; i < rows_to_read; ++i)
    {
        if (null_map[i])
        {
            sets.set(i, scan_spec.filter->testNull());
        }
        else
        {
            auto value = batch_buffer[count++];
            if constexpr (std::is_same_v<SerializedType, Int64>)
                sets.set(i, scan_spec.filter->testInt64(value));
            else if constexpr (std::is_same_v<SerializedType, Int32>)
                sets.set(i, scan_spec.filter->testInt32(value));
            else if constexpr (std::is_same_v<SerializedType, Int16>)
                sets.set(i, scan_spec.filter->testInt16(value));
            else if constexpr (std::is_same_v<SerializedType, Float32>)
                sets.set(i, scan_spec.filter->testFloat32(value));
            else if constexpr (std::is_same_v<SerializedType, Float64>)
                sets.set(i, scan_spec.filter->testFloat64(value));
            else
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported type");
        }
    }
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (row_set)
            row_set.value().setOffset(rows_read);
        readAndDecodePage();
        auto * number_column = static_cast<DataType::ColumnType *>(column.get());
        auto & data = number_column->getData();

        if (plain)
            plain_decoder->decodeFixedValue<typename DataType::FieldType, SerializedType>(data, row_set, rows_can_read);
        else
        {
            if (row_set.has_value() || !batch_buffer.empty())
            {
                nextIdxBatchIfEmpty(rows_can_read);
                decodeFixedValueInternal(data, batch_buffer.data(), row_set, rows_can_read);
            }
            else
            {
                auto old_size = data.size();
                data.resize(old_size + rows_can_read);
                size_t count = idx_decoder.GetBatchWithDict(
                    dict.data(), static_cast<Int32>(dict.size()), data.data() + old_size, static_cast<int>(rows_can_read));
                if (count != rows_can_read)
                    throw DB::Exception(
                        ErrorCodes::LOGICAL_ERROR, "read full idx batch failed. read {} rows, expect {}", count, rows_can_read);
            }

            batch_buffer.resize(0);
            state.offsets.consume(rows_can_read);
        }
        rows_read += rows_can_read;
    }
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (row_set)
            row_set.value().setOffset(rows_read);
        readAndDecodePage();
        auto * number_column = static_cast<DataType::ColumnType *>(column.get());
        auto & data = number_column->getData();
        if (plain)
            plain_decoder->decodeFixedValueSpace<typename DataType::FieldType, SerializedType>(data, row_set, null_map, rows_can_read);
        else
        {
            nextIdxBatchIfEmpty(rows_can_read - null_count);
            dict_decoder->decodeFixedValueSpace(batch_buffer, data, row_set, null_map, rows_can_read);
        }
        rows_read += rows_can_read;
    }
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::readDictPage(const parquet::DictionaryPage & page)
{
    const SerializedType * dict_data = reinterpret_cast<const SerializedType *>(page.data());
    size_t dict_size = page.num_values();
    dict.resize(dict_size);
    if constexpr (std::is_same_v<typename DataType::FieldType, SerializedType>)
        memcpy(dict.data(), dict_data, dict_size * sizeof(typename DataType::FieldType));
    else
        for (size_t i = 0; i < dict_size; i++)
            dict[i] = static_cast<typename DataType::FieldType>(dict_data[i]);
}

template <typename DataType, typename SerializedType>
size_t NumberDictionaryReader<DataType, SerializedType>::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);
    if (plain)
    {
        state.data.checkAndConsume(sizeof(SerializedType) * skipped);
    }
    else
    {
        if (!batch_buffer.empty())
        {
            // only support skip all
            chassert(batch_buffer.size() == skipped);
            batch_buffer.resize(0);
        }
        else
        {
            state.idx_buffer.resize(skipped);
            size_t count = idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(skipped));
            if (count != skipped)
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "skip rows failed. read {} rows, expect {}", count, skipped);
            state.idx_buffer.resize(0);
        }
    }
    return rows_to_skip - skipped;
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::createDictDecoder()
{
    dict_decoder = std::make_unique<DictDecoder>(state.idx_buffer, state.offsets);
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::downgradeToPlain()
{
    dict.resize_exact(0);
    dict_decoder.reset();
}

template <typename DataType>
FixedLengthColumnDirectReader<DataType>::FixedLengthColumnDirectReader(
    PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_)
    : SelectiveColumnReader(std::move(page_reader_creator_), scan_spec_), data_type(datatype_)
{
    element_size = scan_spec_.column_desc->type_length();
}

template <typename DataType>
void FixedLengthColumnDirectReader<DataType>::computeRowSet(std::optional<RowSet> & , size_t )
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "doesn't support compute row set");
}

template <typename DataType>
void FixedLengthColumnDirectReader<DataType>::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        readAndDecodePage();
        if (row_set)
            row_set.value().setOffset(rows_read);
        if constexpr (std::is_same_v<DataType, typename DB::DataTypeFixedString>)
        {
            auto * number_column = static_cast<ColumnFixedString *>(column.get());
            auto & data = number_column->getChars();
            plain_decoder->decodeFixedString(data, row_set, rows_can_read, element_size);
        }
        else
        {
            auto * number_column = static_cast<DataType::ColumnType *>(column.get());
            auto & data = number_column->getData();
            plain_decoder->decodeFixedLengthData<typename DataType::FieldType>(
                data, row_set, rows_can_read, ValueConverterImpl<typename DataType::FieldType>::convert);
        }
        rows_read += rows_can_read;
    }
}

template <typename DataType>
void FixedLengthColumnDirectReader<DataType>::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t , size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        readAndDecodePage();
        if (row_set)
            row_set.value().setOffset(rows_read);
        if constexpr (std::is_same_v<DataType, typename DB::DataTypeFixedString>)
        {
            auto * number_column = static_cast<ColumnFixedString *>(column.get());
            auto & data = number_column->getChars();
            plain_decoder->decodeFixedStringSpace(data, row_set, null_map, rows_can_read, element_size);
        }
        else
        {
            auto * number_column = static_cast<DataType::ColumnType *>(column.get());
            auto & data = number_column->getData();
            plain_decoder->decodeFixedLengthDataSpace(data, row_set, null_map, rows_can_read, ValueConverterImpl<typename DataType::FieldType>::convert);
        }
        rows_read += rows_can_read;
    }
}

template <typename DataType>
size_t FixedLengthColumnDirectReader<DataType>::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);
    state.data.consume(skipped * element_size);
    return rows_to_skip - skipped;
}

template <typename DataType, typename DictValueType>
FixedLengthColumnDictionaryReader<DataType, DictValueType>::FixedLengthColumnDictionaryReader(
    PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_) :
    SelectiveColumnReader(std::move(page_reader_creator_), scan_spec_), data_type(datatype_)
{
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::computeRowSet(OptionalRowSet & , size_t )
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "doesn't support compute row set");
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::downgradeToPlain()
{
    dict_decoder = nullptr;
    dict.clear();
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::readDictPage(const parquet::DictionaryPage & page)
{
    const auto * dict_data = page.data();
    size_t dict_size = page.num_values();
    dict.reserve(dict_size);
    for (size_t i = 0; i < dict_size; i++)
    {
        DictValueType value;
        // for string
        if constexpr (std::is_same_v<DictValueType, String>)
        {
            value.resize(element_size);
            memcpy(value.data(), dict_data, element_size);
        }
        // for decimals
        else
        {
            ValueConverterImpl<DictValueType>::convert(dict_data, reinterpret_cast<uint8_t *>(&value));
        }
        dict.emplace_back(value);
        dict_data += element_size;
    }
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::createDictDecoder()
{
    dict_decoder = std::make_unique<DictDecoder>(state.idx_buffer, state.offsets);
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::nextIdxBatchIfEmpty(size_t rows_to_read)
{
    if (!state.idx_buffer.empty() || plain)
        return;
    state.idx_buffer.resize(rows_to_read);
    idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(rows_to_read));
}

template <typename DataType, typename DictValueType>
size_t FixedLengthColumnDictionaryReader<DataType, DictValueType>::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);
    if (plain)
    {
        state.data.checkAndConsume(element_size * skipped);
    }
    else
    {
        state.idx_buffer.resize(skipped);
        size_t count = idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(skipped));
        if (count != skipped)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "skip rows failed. read {} rows, expect {}", count, skipped);
        state.idx_buffer.resize(0);
    }
    return rows_to_skip - skipped;
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t , size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (row_set)
            row_set.value().setOffset(rows_read);
        readAndDecodePage();
        if (plain)
        {
            if constexpr (std::is_same_v<DataType, typename DB::DataTypeFixedString>)
            {
                auto * string_column = static_cast<ColumnFixedString *>(column.get());
                plain_decoder->decodeFixedStringSpace(string_column->getChars(), row_set, null_map, rows_can_read, element_size);
            }
            else
            {
                auto * number_column = static_cast<DataType::ColumnType *>(column.get());
                auto & data = number_column->getData();
                plain_decoder->decodeFixedLengthDataSpace(data, row_set, null_map, rows_can_read, ValueConverterImpl<typename DataType::FieldType>::convert);
            }
        }
        else
        {
            nextIdxBatchIfEmpty(rows_can_read);
            if constexpr (std::is_same_v<DataType, typename DB::DataTypeFixedString>)
            {
                auto * string_column = static_cast<ColumnFixedString *>(column.get());
                dict_decoder->decodeFixedStringSpace(dict, string_column->getChars(), row_set, null_map, rows_can_read);
            }
            else
            {
                auto * number_column = static_cast<DataType::ColumnType *>(column.get());
                auto & data = number_column->getData();
                dict_decoder->decodeFixedLengthDataSpace(dict, data, row_set, null_map, rows_can_read);
            }
        }
        rows_read += rows_can_read;
    }
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (row_set)
            row_set.value().setOffset(rows_read);
        readAndDecodePage();
        if (plain)
        {
            if constexpr (std::is_same_v<DictValueType, String>)
            {
                auto * string_column = static_cast<ColumnFixedString *>(column.get());
                plain_decoder->decodeFixedString(string_column->getChars(), row_set, rows_can_read, element_size);
            }
            else
            {
                auto * number_column = static_cast<DataType::ColumnType *>(column.get());
                auto & data = number_column->getData();
                plain_decoder->decodeFixedLengthData(data, row_set, rows_can_read, ValueConverterImpl<typename DataType::FieldType>::convert);
            }
        }
        else
        {
            nextIdxBatchIfEmpty(rows_can_read);
            if constexpr (std::is_same_v<DictValueType, String>)
            {
                auto * string_column = static_cast<ColumnFixedString *>(column.get());
                dict_decoder->decodeFixedString(dict, string_column->getChars(), row_set, rows_can_read);
            }
            else
            {
                auto * number_column = static_cast<DataType::ColumnType *>(column.get());
                auto & data = number_column->getData();
                dict_decoder->decodeFixedLengthData(dict, data, row_set, rows_can_read);
            }
        }
        rows_read += rows_can_read;
    }
}

void OptionalColumnReader::nextBatchNullMapIfNeeded(size_t rows_to_read)
{
    if (!cur_null_map.empty())
        return;
    cur_null_map.resize(rows_to_read);
    std::fill(cur_null_map.begin(), cur_null_map.end(), 0);
    cur_null_count = 0;
    const auto & def_levels = child->getDefinitionLevels();
    size_t start = def_levels.size() - child->availableRows();
    int16_t max_def_level = maxDefinitionLevel();
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

void OptionalColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    applyLazySkip();
    size_t rows_read = 0;
    auto * nullable_column = static_cast<ColumnNullable *>(column.get());
    auto nested_column = nullable_column->getNestedColumnPtr()->assumeMutable();
    auto & null_data = nullable_column->getNullMapData();
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, child->availableRows());
        if (row_set)
            row_set.value().setOffset(rows_read);
        nextBatchNullMapIfNeeded(rows_can_read);
        if (row_set.has_value())
        {
            const auto & sets = row_set.value();
            for (size_t i = 0; i < rows_can_read; i++)
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
            child->readSpace(nested_column, row_set, cur_null_map, cur_null_count, rows_can_read);
        }
        else
        {
            child->read(nested_column, row_set, rows_can_read);
        }
        cleanNullMap();
        rows_read += rows_can_read;
    }
}

size_t OptionalColumnReader::skipValuesInCurrentPage(size_t rows)
{
    if (!rows)
        return 0;
    if (!child->state.offsets.remain_rows || !child->state.page)
        return rows;
    auto skipped = std::min(rows, child->state.offsets.remain_rows);
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
size_t OptionalColumnReader::availableRows() const
{
    return child->availableRows() - state.lazy_skip_rows;
}

template class NumberColumnDirectReader<DataTypeInt8, Int32>;
template class NumberColumnDirectReader<DataTypeInt16, Int32>;
template class NumberColumnDirectReader<DataTypeInt32, Int32>;
template class NumberColumnDirectReader<DataTypeInt64, Int64>;
template class NumberColumnDirectReader<DataTypeUInt8, Int32>;
template class NumberColumnDirectReader<DataTypeUInt16, Int32>;
template class NumberColumnDirectReader<DataTypeUInt32, Int32>;
template class NumberColumnDirectReader<DataTypeUInt64, Int64>;
template class NumberColumnDirectReader<DataTypeFloat32, Float32>;
template class NumberColumnDirectReader<DataTypeFloat64, Float64>;
template class NumberColumnDirectReader<DataTypeDate32, Int32>;
template class NumberColumnDirectReader<DataTypeDate, Int32>;
template class NumberColumnDirectReader<DataTypeDateTime, Int32>;
template class NumberColumnDirectReader<DataTypeDateTime64, Int64>;
template class NumberColumnDirectReader<DataTypeDateTime, Int64>;
template class FixedLengthColumnDirectReader<DataTypeFixedString>;
template class FixedLengthColumnDirectReader<DataTypeDecimal128>;
template class FixedLengthColumnDirectReader<DataTypeDecimal256>;


template class NumberDictionaryReader<DataTypeInt8, Int32>;
template class NumberDictionaryReader<DataTypeInt16, Int32>;
template class NumberDictionaryReader<DataTypeInt32, Int32>;
template class NumberDictionaryReader<DataTypeInt64, Int64>;
template class NumberDictionaryReader<DataTypeUInt8, Int32>;
template class NumberDictionaryReader<DataTypeUInt16, Int32>;
template class NumberDictionaryReader<DataTypeUInt32, Int32>;
template class NumberDictionaryReader<DataTypeUInt64, Int64>;
template class NumberDictionaryReader<DataTypeFloat32, Float32>;
template class NumberDictionaryReader<DataTypeFloat64, Float64>;
template class NumberDictionaryReader<DataTypeDate32, Int32>;
template class NumberDictionaryReader<DataTypeDate, Int32>;
template class NumberDictionaryReader<DataTypeDateTime, Int32>;
template class NumberDictionaryReader<DataTypeDateTime64, Int64>;
template class NumberDictionaryReader<DataTypeDateTime, Int64>;
template class FixedLengthColumnDictionaryReader<DataTypeFixedString, String>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal128, Decimal128>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal256, Decimal256>;

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
        if (len == 0)
            sets.set(i, filter->testString(""));
        else
            sets.set(i, filter->testString(String(reinterpret_cast<const char *>(start + offset), len)));
        offset += len;
    }
}
void computeRowSetPlainStringSpace(
    const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read, PaddedPODArray<UInt8> & null_map)
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
        if (len == 0)
            sets.set(i, filter->testString(""));
        else
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
    state.data.checkSize(1);
    uint8_t bit_width = *state.data.buffer;
    state.data.consume(1);
    idx_decoder = arrow::util::RleDecoder(state.data.buffer, static_cast<int>(state.data.buffer_size), bit_width);
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
        if (len)
        {
            String value;
            value.resize(len);
            memcpy(value.data(), dict_data, len);
            dict.emplace_back(value);
        }
        else
            dict.emplace_back("");
        dict_data += len;
    }
}

size_t StringDictionaryReader::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);
    if (plain)
    {
        size_t offset = 0;
        for (size_t i = 0; i < skipped; i++)
        {
            auto len = loadLength(state.data.buffer + offset);
            offset += 4 + len;
        }
        state.data.checkAndConsume(offset);
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
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (row_set)
            row_set.value().setOffset(rows_read);
        readAndDecodePage();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        if (plain)
        {
            size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.data, row_set, null_map, rows_can_read);
            string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_can_read);
            string_column->getChars().reserve(string_column->getChars().size() + total_size);
            plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_can_read);
        }
        else
        {
            auto nonnull_count = rows_can_read - null_count;
            nextIdxBatchIfEmpty(nonnull_count);
            dict_decoder->decodeStringSpace(dict, string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_can_read);
        }
        rows_read += rows_can_read;
    }
}

void StringDictionaryReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (row_set)
            row_set.value().setOffset(rows_read);
        readAndDecodePage();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        if (plain)
        {
            size_t total_size = plain_decoder->calculateStringTotalSize(state.data, row_set, rows_can_read);
            string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_can_read);
            string_column->getChars().reserve(string_column->getChars().size() + total_size);
            plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_can_read);
        }
        else
        {
            nextIdxBatchIfEmpty(rows_can_read);
            dict_decoder->decodeString(dict, string_column->getChars(), string_column->getOffsets(), row_set, rows_can_read);
        }
        rows_read += rows_can_read;
    }
}

void StringDictionaryReader::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    readAndDecodePage();
    chassert(rows_to_read <= state.offsets.remain_rows);
    if (plain)
    {
        computeRowSetPlainStringSpace(state.data.buffer, row_set, scan_spec.filter, rows_to_read, null_map);
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
    if (!scan_spec.filter || !row_set.has_value())
        return;
    readAndDecodePage();
    chassert(rows_to_read <= state.offsets.remain_rows);
    if (plain)
    {
        computeRowSetPlainString(state.data.buffer, row_set, scan_spec.filter, rows_to_read);
        return;
    }
    nextIdxBatchIfEmpty(rows_to_read);
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

void StringDictionaryReader::downgradeToPlain()
{
    dict_decoder = nullptr;
    dict.clear();
}

size_t StringDirectReader::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);
    size_t offset = 0;
    for (size_t i = 0; i < skipped; i++)
    {
        auto len = loadLength(state.data.buffer + offset);
        offset += 4 + len;
    }
    state.data.checkAndConsume(offset);
    return rows_to_skip - skipped;
}

void StringDirectReader::readSpace(
    MutableColumnPtr & column,
    OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    size_t null_count,
    size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (row_set)
            row_set.value().setOffset(rows_read);
        readAndDecodePage();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.data, row_set, null_map, rows_to_read - null_count);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
        rows_read += rows_can_read;
    }
}

void StringDirectReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (row_set)
            row_set.value().setOffset(rows_read);
        readAndDecodePage();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        size_t total_size = plain_decoder->calculateStringTotalSize(state.data, row_set, rows_can_read);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_can_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_can_read);
        rows_read += rows_can_read;
    }
}

void StringDirectReader::computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    readAndDecodePage();
    computeRowSetPlainStringSpace(state.data.buffer, row_set, scan_spec.filter, rows_to_read, null_map);
}

void StringDirectReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePage();
    computeRowSetPlainString(state.data.buffer, row_set, scan_spec.filter, rows_to_read);
}

static void appendString(ColumnString::Chars & chars, IColumn::Offsets & offsets, const String & value)
{
    if (!value.empty())
    {
        auto chars_cursor = chars.size();
        chars.resize(chars_cursor + value.size() + 1);
        memcpySmallAllowReadWriteOverflow15(&chars[chars_cursor], value.data(), value.size());
        chars.back() = 0;
    }
    else
        chars.push_back(0);
    offsets.push_back(chars.size());
}

void DictDecoder::decodeStringSpace(
    std::vector<String> & dict,
    ColumnString::Chars & chars,
    IColumn::Offsets & string_offsets,
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
                    string_offsets.push_back(chars.size());
                }
                else
                {
                    const String & value = dict[idx_buffer[count]];
                    appendString(chars, string_offsets, value);
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
                string_offsets.push_back(chars.size());
            }
            else
            {
                const String & value = dict[idx_buffer[count]];
                appendString(chars, string_offsets, value);
                count++;
            }
            rows_read++;
        }
    }
    chassert(count == idx_buffer.size());
    this->offsets.consume(rows_to_read);
    idx_buffer.resize(0);
}

void DictDecoder::decodeString(
    std::vector<String> & dict,
    ColumnString::Chars & chars,
    IColumn::Offsets & string_offsets,
    const OptionalRowSet & row_set,
    size_t rows_to_read)
{
    const bool has_set = row_set.has_value();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!has_set || row_set.value().get(i))
        {
            const String & value = dict[idx_buffer[i]];
            appendString(chars, string_offsets, value);
        }
    }
    idx_buffer.resize(0);
    this->offsets.consume(rows_to_read);
}

void DictDecoder::decodeFixedString(
    PaddedPODArray<String> & dict, ColumnFixedString::Chars & chars, const OptionalRowSet & row_set, size_t rows_to_read)
{
    const bool has_set = row_set.has_value();
    chars.reserve(chars.size() + rows_to_read * dict[0].size());
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!has_set || row_set.value().get(i))
        {
            const String & value = dict[idx_buffer[i]];
            chars.insert_assume_reserved(value.data(), value.data() + value.size());
        }
    }
    idx_buffer.resize(0);
    this->offsets.consume(rows_to_read);
}

template <class DictValueType>
void DictDecoder::decodeFixedLengthData(
    PaddedPODArray<DictValueType> & dict,
    PaddedPODArray<DictValueType> & data, const OptionalRowSet & row_set, size_t rows_to_read)
{
    const bool has_set = row_set.has_value();
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!has_set || row_set.value().get(i))
        {
            data.push_back(dict[idx_buffer[i]]);
        }
    }
    idx_buffer.resize(0);
    this->offsets.consume(rows_to_read);
}

template <class DictValueType>
void DictDecoder::decodeFixedValue(
    PaddedPODArray<DictValueType> & dict, PaddedPODArray<DictValueType> & data, const OptionalRowSet & row_set, size_t rows_to_read)
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
    this->offsets.consume(rows_to_read);
}

template <class DictValueType>
void DictDecoder::decodeFixedValueSpace(
    PaddedPODArray<DictValueType> & batch_buffer,
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
                    data.push_back(batch_buffer[count++]);
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
                data.push_back(batch_buffer[count++]);
            rows_read++;
        }
    }
    chassert(count == batch_buffer.size());
    this->offsets.consume(rows_to_read);
    idx_buffer.resize(0);
    batch_buffer.resize(0);
}

void DictDecoder::decodeFixedStringSpace(
    PaddedPODArray<String> & dict,
    ColumnFixedString::Chars & chars,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    size_t rows_to_read)
{
    size_t rows_read = 0;
    size_t count = 0;
    size_t element_size = dict.front().size();
    chars.reserve(chars.size() + rows_to_read * element_size);
    const bool has_set = row_set.has_value();
    while (rows_read < rows_to_read)
    {
        if (!has_set || row_set.value().get(rows_read))
        {
            if (null_map[rows_read])
                chars.resize(chars.size() + element_size);
            else
            {
                auto & value = dict[idx_buffer[count]];
                chars.insert_assume_reserved(value.data(), value.data() + element_size);
            }
        }
        else if (!null_map[rows_read])
            count++;
        rows_read++;
    }
    this->offsets.consume(rows_to_read);
    idx_buffer.resize(0);
}

template <class DictValueType>
void DictDecoder::decodeFixedLengthDataSpace(
    PaddedPODArray<DictValueType> & dict,
    PaddedPODArray<DictValueType> & data,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    size_t rows_to_read)
{
    size_t rows_read = 0;
    size_t count = 0;
    const bool has_set = row_set.has_value();
    while (rows_read < rows_to_read)
    {
        if (!has_set || row_set.value().get(rows_read))
        {
            if (null_map[rows_read])
                data.push_back(0);
            else
            {
                data.push_back(dict[idx_buffer[count]]);
            }
        }
        else if (!null_map[rows_read])
            count++;
        rows_read++;
    }
    this->offsets.consume(rows_to_read);
    idx_buffer.resize(0);
}

void PlainDecoder::decodeString(
    ColumnString::Chars & chars, IColumn::Offsets & string_offsets, const OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t offset = 0;
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        for (size_t i = 0; i < rows_to_read; i++)
        {
            auto len = loadLength(page_data.buffer + offset);
            offset += 4;
            if (sets.get(i))
            {
                if (len)
                    chars.insert_assume_reserved(page_data.buffer + offset, page_data.buffer + offset + len);
                chars.push_back(0);
                string_offsets.push_back(chars.size());
                offset += len;
            }
            else
            {
                offset += len;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            auto len = loadLength(page_data.buffer + offset);
            offset += 4;
            if (len)
                chars.insert_assume_reserved(page_data.buffer + offset, page_data.buffer + offset + len);
            chars.push_back(0);
            string_offsets.push_back(chars.size());
            offset += len;
        }
    }
    page_data.checkAndConsume(offset);
    this->offsets.consume(rows_to_read);
}

void PlainDecoder::decodeStringSpace(
    ColumnString::Chars & chars,
    IColumn::Offsets & string_offsets,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8, 4096> & null_map,
    size_t rows_to_read)
{
    size_t offset = 0;
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (null_map[i])
            {
                if (sets.get(i))
                {
                    // null string
                    chars.push_back(0);
                    string_offsets.push_back(chars.size());
                }
                continue;
            }
            auto len = loadLength(page_data.buffer + offset);
            offset += 4;
            if (sets.get(i))
            {
                if (len)
                    chars.insert_assume_reserved(page_data.buffer + offset, page_data.buffer + offset + len);
                chars.push_back(0);
                string_offsets.push_back(chars.size());
                offset += len;
            }
            else
            {
                offset += len;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (null_map[i])
            {
                chars.push_back(0);
                string_offsets.push_back(chars.size());
                continue;
            }
            auto len = loadLength(page_data.buffer + offset);
            offset += 4;
            if (len)
                chars.insert_assume_reserved(page_data.buffer + offset, page_data.buffer + offset + len);
            chars.push_back(0);
            string_offsets.push_back(chars.size());
            offset += len;
        }
    }
    page_data.checkAndConsume(offset);
    this->offsets.consume(rows_to_read);
}
size_t PlainDecoder::calculateStringTotalSize(const ParquetData & data, const OptionalRowSet & row_set, const size_t rows_to_read)
{
    size_t offset = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < rows_to_read; i++)
    {
        addOneString(false, data, offset, row_set, i, total_size);
    }
    return total_size;
}
size_t DB::PlainDecoder::calculateStringTotalSizeSpace(
    const ParquetData & data, const DB::OptionalRowSet & row_set, DB::PaddedPODArray<UInt8> & null_map, const size_t rows_to_read)
{
    size_t offset = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < rows_to_read; i++)
    {
        addOneString(null_map[i], data, offset, row_set, i, total_size);
    }
    return total_size;
}

void PlainDecoder::decodeFixedString(ColumnFixedString::Chars & data, const OptionalRowSet & row_set, size_t rows_to_read, size_t n)
{
    data.reserve(data.size() + (n * rows_to_read));
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!row_set || row_set.value().get(i))
        {
            page_data.checkSize(n);
            data.insert_assume_reserved(page_data.buffer, page_data.buffer + n);
            page_data.consume(n);
        }
        else
            page_data.checkAndConsume(n);
    }
}

template <typename T>
void PlainDecoder::decodeFixedLengthData(PaddedPODArray<T> & data, const OptionalRowSet & row_set, size_t rows_to_read, ValueConverter value_converter)
{
    auto element_size = sizeof(T);
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!row_set || row_set.value().get(i))
        {
            page_data.checkSize(element_size);
            value_converter(page_data.buffer, reinterpret_cast<uint8_t *  >(data.data()));
            data.resize(data.size() + 1);
            page_data.consume(element_size);
        }
        else
            page_data.checkAndConsume(element_size);
    }
}

void PlainDecoder::decodeFixedStringSpace(
    ColumnFixedString::Chars & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read, size_t n)
{
    data.reserve(data.size() + (n * rows_to_read));
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!row_set || row_set.value().get(i))
        {
            if (null_map[i])
            {
                data.resize(data.size() + n);
            }
            else
            {
                page_data.checkSize(n);
                data.insert_assume_reserved(page_data.buffer, page_data.buffer + n);
                page_data.consume(n);
            }
        }
        else
        {
            if (!null_map[i])
                page_data.checkAndConsume(n);
        }
    }
}
template <typename T>
void PlainDecoder::decodeFixedLengthDataSpace(
    PaddedPODArray<T> & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read, ValueConverter value_converter)
{
    auto element_size = sizeof(T);
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!row_set || row_set.value().get(i))
        {
            if (null_map[i])
            {
                data.resize(data.size() + 1);
            }
            else
            {
                page_data.checkSize(element_size);
                value_converter(page_data.buffer, reinterpret_cast<uint8_t *  >(data.data()));
                data.resize(data.size() + 1);
                page_data.consume(element_size);
            }
        }
        else
        {
            if (!null_map[i])
                page_data.checkAndConsume(element_size);
        }
    }
}

template <typename T, typename S>
void PlainDecoder::decodeFixedValueSpace(
    PaddedPODArray<T> & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
{
    size_t rows_read = 0;
    const S * start = reinterpret_cast<const S *>(page_data.buffer);
    size_t count = 0;
    if (!row_set.has_value())
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (null_map[i])
            {
                data.push_back(0);
            }
            else
            {
                data.push_back(static_cast<T>(start[count]));
                count++;
            }
        }
    }
    else
    {
        const auto & sets = row_set.value();
        while (rows_read < rows_to_read)
        {
            if (sets.get(rows_read))
            {
                if (null_map[rows_read])
                {
                    data.push_back(0);
                }
                else
                {
                    data.push_back(static_cast<T>(start[count]));
                    count++;
                }
            }
            rows_read++;
        }
    }
    page_data.checkAndConsume(count * sizeof(S));
    offsets.consume(rows_to_read);
}

static void insertManyToFilter(PaddedPODArray<bool> &filter, bool value, size_t count)
{
    filter.resize(filter.size() + count);
    std::fill(filter.end() - count, filter.end(), value);
}


void ListColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    // support list inside nullable ?
    NullMap * null_map = nullptr;
    MutableColumnPtr& nested_column = column;
    if (column->isNullable())
    {
        ColumnNullable* null_column = static_cast<ColumnNullable *>(column.get());
        null_map = &null_column->getNullMapData();
        nested_column = null_column->getNestedColumnPtr()->assumeMutable();
    }
    if (!checkColumn<ColumnArray>(*nested_column))
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "column type should be array, but is {}", nested_column->getName());
    }
    ColumnArray* array_column = static_cast<ColumnArray *>(nested_column.get());

    const bool has_filter = row_set.has_value();
    size_t valid_count = has_filter ? row_set->count() : rows_to_read;
    auto & offsets = array_column->getOffsets();
    auto target_size = offsets.size() + valid_count;
    offsets.reserve(target_size);

    size_t rows_read = 0;
    // read data from multiple pages
    while (rows_read < rows_to_read)
    {
        int array_size = 0;
        size_t old_max_offset = offsets.back();
        const auto & def_levels = child->getDefinitionLevels();
        bool has_def_level = !def_levels.empty();
        const auto & rep_levels = child->getRepetitionLevels();
        size_t start = child->levelsOffset();
        size_t levels_size = rep_levels.size();
        if (levels_size == 0)
            break;
        size_t count = 0;

        PaddedPODArray<bool> child_filter;
        if (has_filter)
            child_filter.reserve(rows_to_read);
        // read from current page
        while (true)
        {
            size_t idx = start + count;
            if (idx >= levels_size || rows_read >= rows_to_read)
                break;
            if (rep_levels[idx] <= rep_level)
            {
                if (count)
                {
                    rows_read++;
                    if (has_filter)
                    {
                        auto valid = row_set->get(rows_read - 1);
                        if (valid)
                        {
                            insertManyToFilter(child_filter, true, array_size);
                            offsets.push_back(offsets.back() + array_size);
                        }
                        else
                            insertManyToFilter(child_filter, false, array_size);
                    }
                    else
                        offsets.push_back(offsets.back() + array_size);
                    array_size = 0;
                }
                if (has_def_level && def_levels[idx] < def_level)
                {
                    // value is null
                    if (null_map)
                    {
                        null_map->data()[offsets.size()] = 1;
                    }
                    else
                        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "null value is not supported");
                }
                else
                {
                    array_size++;
                }
            }
            else
                array_size++;
            count++;
        }

        // read tail record
        if (rows_read < rows_to_read)
        {
            if (has_filter)
            {
                auto valid = row_set->get(rows_read);
                if (valid)
                {
                    insertManyToFilter(child_filter, true, array_size);
                    offsets.push_back(offsets.back() + array_size);
                }
                else
                    insertManyToFilter(child_filter, false, array_size);
            }
            else
            {
                offsets.push_back(offsets.back() + array_size);
            }
            rows_read ++;
        }
        auto data_column = array_column->getDataPtr()->assumeMutable();
        OptionalRowSet filter;
        if (has_filter)
            filter = RowSet(child_filter);
        child->read(data_column, filter, has_filter ? filter->totalRows() : offsets.back() - old_max_offset);
    }
}
void ListColumnReader::computeRowSet(std::optional<RowSet> & , size_t )
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}
MutableColumnPtr ListColumnReader::createColumn()
{
    return ColumnArray::create(child->createColumn(), ColumnArray::ColumnOffsets::create());
}
size_t ListColumnReader::skipValuesInCurrentPage(size_t )
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unimplemented operation");
}

size_t ListColumnReader::availableRows() const
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}
void MapColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    // support map inside nullable, how parquet serialize null in map type?
    NullMap * null_map = nullptr;
    MutableColumnPtr& column_inside_nullable = column;
    if (column->isNullable())
    {
        ColumnNullable* null_column = static_cast<ColumnNullable *>(column.get());
        null_map = &null_column->getNullMapData();
        column_inside_nullable = null_column->getNestedColumnPtr()->assumeMutable();
    }
    if (!checkColumn<ColumnMap>(*column_inside_nullable))
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "column type should be map, but is {}", column_inside_nullable->getName());
    }
    ColumnMap* map_column = static_cast<ColumnMap *>(column_inside_nullable.get());
    ColumnArray* array_in_map_column = &map_column->getNestedColumn();
    auto data_column = array_in_map_column->getDataPtr();
    const ColumnTuple & tuple_col = checkAndGetColumn<ColumnTuple>(*data_column);
    if (tuple_col.getColumns().size() !=2)
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "map column should have 2 columns, but has {}", tuple_col.getColumns().size());
    }
    auto key_column = tuple_col.getColumns()[0]->assumeMutable();
    auto value_column = tuple_col.getColumns()[1]->assumeMutable();
    auto & offsets = array_in_map_column->getOffsets();
    size_t valid_count = row_set ? row_set->count() : rows_to_read;
    auto target_size = offsets.size() + valid_count;
    offsets.reserve(target_size);
    // generate offsets;
    const bool has_filter = row_set.has_value();
    size_t rows_read = 0;
    // read data from multiple pages
    while (rows_read < rows_to_read)
    {
        int array_size = 0;
        size_t old_max_offset = offsets.back();
        const auto & def_levels = key_reader->getDefinitionLevels();
        bool has_def_level = !def_levels.empty();
        const auto & rep_levels = key_reader->getRepetitionLevels();
        size_t start = key_reader->levelsOffset();
        size_t levels_size = rep_levels.size();
        if (levels_size == 0)
            break;
        size_t count = 0;

        PaddedPODArray<bool> child_filter;
        if (has_filter)
            child_filter.reserve(rows_to_read);
        while (true)
        {
            size_t idx = start + count;
            if (idx >= levels_size || rows_read >= rows_to_read)
                break;
            if (rep_levels[idx] <= rep_level)
            {
                if (count)
                {
                    rows_read++;
                    if (has_filter)
                    {
                        auto valid = row_set->get(rows_read - 1);
                        if (valid)
                        {
                            insertManyToFilter(child_filter, true, array_size);
                            offsets.push_back(offsets.back() + array_size);
                        }
                        else
                            insertManyToFilter(child_filter, false, array_size);
                    }
                    else
                        offsets.push_back(offsets.back() + array_size);
                    array_size = 0;
                }
                if (has_def_level && def_levels[idx] < def_level)
                {
                    // value is null
                    if (null_map)
                    {
                        null_map->data()[offsets.size()] = 1;
                    }
                    else
                        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "null value is not supported");
                }
                else
                    array_size++;
            }
            else
                array_size++;
            count++;
        }

        // read tail record
        if (rows_read < rows_to_read)
        {
            if (has_filter)
            {
                auto valid = row_set->get(rows_read);
                if (valid)
                {
                    insertManyToFilter(child_filter, true, array_size);
                    offsets.push_back(offsets.back() + array_size);
                }
                else
                    insertManyToFilter(child_filter, false, array_size);
            }
            else
            {
                offsets.push_back(offsets.back() + array_size);
            }
            rows_read ++;
        }
        OptionalRowSet filter;
        if (has_filter)
            filter = RowSet(child_filter);
        key_reader->read(key_column, filter, has_filter ? filter->totalRows() : offsets.back() - old_max_offset);
        value_reader->read(value_column, filter, has_filter ? filter->totalRows() : offsets.back() - old_max_offset);
    }
}
void MapColumnReader::computeRowSet(std::optional<RowSet> & , size_t )
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}
MutableColumnPtr MapColumnReader::createColumn()
{
    MutableColumns columns;
    columns.push_back(key_reader->createColumn());
    columns.push_back(value_reader->createColumn());
    MutableColumnPtr tuple = ColumnTuple::create(std::move(columns));
    MutableColumnPtr array = ColumnArray::create(std::move(tuple));
    return ColumnMap::create(std::move(array));
}

size_t MapColumnReader::skipValuesInCurrentPage(size_t )
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unimplemented operation");
}
size_t MapColumnReader::availableRows() const
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}

void StructColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    /// TODO support tuple inside nullable
    checkColumn<ColumnTuple>(*column);
    ColumnTuple * tuple_column = static_cast<ColumnTuple*>(column.get());
    const auto *tuple_type = checkAndGetDataType<DataTypeTuple>(structType.get());
    auto names = tuple_type->getElementNames();
    for (size_t i = 0; i < names.size(); i++)
    {
        auto nested_column = tuple_column->getColumn(i).assumeMutable();
        auto & nested_reader = children.at(names.at(i));
        nested_reader->read(nested_column, row_set, rows_to_read);
    }
}

void StructColumnReader::computeRowSet(std::optional<RowSet> & , size_t )
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}

MutableColumnPtr StructColumnReader::createColumn()
{
    return structType->createColumn();
}

size_t StructColumnReader::skipValuesInCurrentPage(size_t count)
{
    size_t remain_rows = 0;
    for (auto & child : children)
    {
        auto rows = child.second->skipValuesInCurrentPage(count);
        if (remain_rows && remain_rows != rows)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "skip values in struct column reader failed");
        }
        else
            remain_rows = rows;
    }
    return remain_rows;
}

size_t StructColumnReader::availableRows() const
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}
}
