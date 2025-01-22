#include "SelectiveColumnReader.h"

#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
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
    if (!row_set)
    {
        if constexpr (std::is_same_v<T, S>)
        {
            data.insert(start, start + rows_to_read);
        }
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

void PlainDecoder::decodeBoolean(
    PaddedPODArray<UInt8> & data, PaddedPODArray<UInt8> & src, const OptionalRowSet & row_set, size_t rows_to_read)
{
    decodeFixedValueInternal(data, src.data(), row_set, rows_to_read);
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
    state.rep_levels.resize(0);
    state.def_levels.resize(0);
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
        state.offsets.reset(page_header.data_page_header.num_values);
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
    //    state.offsets.remain_rows = page.num_values();
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
    if (!rows_to_skip)
        return;
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
void NumberColumnDirectReader<DataType, SerializedType>::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        auto * number_column = static_cast<DataType::ColumnType *>(column.get());
        auto & data = number_column->getData();
        plain_decoder->decodeFixedValue<typename DataType::FieldType, SerializedType>(data, row_set, rows_can_read);
        if (row_set)
            row_set->addOffset(rows_can_read);
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
        readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        auto * number_column = static_cast<DataType::ColumnType *>(column.get());
        auto & data = number_column->getData();
        plain_decoder->decodeFixedValueSpace<typename DataType::FieldType, SerializedType>(data, row_set, null_map, rows_can_read);
        if (row_set)
            row_set->addOffset(rows_can_read);
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
    : SelectiveColumnReader(page_reader_creator_, scan_spec_), datatype(datatype_)
{
}

template <typename DataType, typename SerializedType>
void NumberDictionaryReader<DataType, SerializedType>::nextIdxBatchIfEmpty(size_t rows_to_read)
{
    if (!batch_buffer.empty() || plain)
        return;
    batch_buffer.resize(rows_to_read);
    if (!rows_to_read)
        return;
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
        readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
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
        if (row_set)
            row_set->addOffset(rows_can_read);
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
        readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        auto * number_column = static_cast<DataType::ColumnType *>(column.get());
        auto & data = number_column->getData();
        if (plain)
            plain_decoder->decodeFixedValueSpace<typename DataType::FieldType, SerializedType>(data, row_set, null_map, rows_can_read);
        else
        {
            nextIdxBatchIfEmpty(rows_can_read - null_count);
            dict_decoder->decodeFixedValueSpace(batch_buffer, data, row_set, null_map, rows_can_read);
        }
        if (row_set)
            row_set->addOffset(rows_can_read);
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
    if (scan_spec_.column_desc->type_length())
        element_size = scan_spec_.column_desc->type_length();
    else if (scan_spec_.column_desc->physical_type() == parquet::Type::INT96)
        element_size = 12;
    else
        element_size = data_type->getSizeOfValueInMemory();
}

template <typename DataType>
ValueConverter FixedLengthColumnDirectReader<DataType>::getConverter()
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "doesn't support get converter from type {}", data_type->getName());
}

template <>
ValueConverter FixedLengthColumnDirectReader<DataTypeDecimal32>::getConverter()
{
    return ValueConverterImpl<Decimal32>::convert;
}

template <>
ValueConverter FixedLengthColumnDirectReader<DataTypeDecimal64>::getConverter()
{
    return ValueConverterImpl<Decimal64>::convert;
}

template <>
ValueConverter FixedLengthColumnDirectReader<DataTypeDecimal128>::getConverter()
{
    return ValueConverterImpl<Decimal128>::convert;
}

template <>
ValueConverter FixedLengthColumnDirectReader<DataTypeDecimal256>::getConverter()
{
    return ValueConverterImpl<Decimal256>::convert;
}

template <>
ValueConverter FixedLengthColumnDirectReader<DataTypeDateTime64>::getConverter()
{
    const DataTypeDateTime64 * type = static_cast<const DataTypeDateTime64 *>(data_type.get());
    switch (type->getScale())
    {
        case 0:
            return ValueConverterImpl<DateTime64, 0>::convert;
        case 3:
            return ValueConverterImpl<DateTime64, 3>::convert;
        case 6:
            return ValueConverterImpl<DateTime64, 6>::convert;
        case 9:
            return ValueConverterImpl<DateTime64, 9>::convert;
        default:
            throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported scale {}", type->getScale());
    }
}

template <>
ValueConverter FixedLengthColumnDirectReader<DataTypeInt64>::getConverter()
{
    return ValueConverterImpl<DateTime64, 0>::convert;
}

template <typename DataType>
void FixedLengthColumnDirectReader<DataType>::computeRowSet(std::optional<RowSet> &, size_t)
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "doesn't support compute row set");
}

template <typename DataType>
void FixedLengthColumnDirectReader<DataType>::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
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
            plain_decoder->decodeFixedLengthData<typename DataType::FieldType>(data, row_set, rows_can_read, element_size, getConverter());
        }
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

template <typename DataType>
void FixedLengthColumnDirectReader<DataType>::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
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
            plain_decoder->decodeFixedLengthDataSpace(data, row_set, null_map, rows_can_read, element_size, getConverter());
        }
        if (row_set)
            row_set->addOffset(rows_can_read);
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
    PageReaderCreator page_reader_creator_, ScanSpec scan_spec_, DataTypePtr datatype_)
    : SelectiveColumnReader(std::move(page_reader_creator_), scan_spec_), data_type(datatype_)
{
    if (scan_spec_.column_desc->type_length())
        element_size = scan_spec_.column_desc->type_length();
    else if (scan_spec_.column_desc->physical_type() == parquet::Type::INT96)
        element_size = 12;
    else
        element_size = data_type->getSizeOfValueInMemory();
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::computeRowSet(OptionalRowSet &, size_t)
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
            static auto converter = getConverter();
            converter(dict_data, element_size, reinterpret_cast<uint8_t *>(&value));
        }
        dict.emplace_back(value);
        dict_data += element_size;
    }
}

template <typename DataType, typename DictValueType>
ValueConverter FixedLengthColumnDictionaryReader<DataType, DictValueType>::getConverter()
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "doesn't support get converter from type {}", data_type->getName());
}

template <>
ValueConverter FixedLengthColumnDictionaryReader<DataTypeDecimal32, Decimal32>::getConverter()
{
    return ValueConverterImpl<Decimal32>::convert;
}

template <>
ValueConverter FixedLengthColumnDictionaryReader<DataTypeDecimal64, Decimal64>::getConverter()
{
    return ValueConverterImpl<Decimal64>::convert;
}

template <>
ValueConverter FixedLengthColumnDictionaryReader<DataTypeDecimal128, Decimal128>::getConverter()
{
    return ValueConverterImpl<Decimal128>::convert;
}

template <>
ValueConverter FixedLengthColumnDictionaryReader<DataTypeDecimal256, Decimal256>::getConverter()
{
    return ValueConverterImpl<Decimal256>::convert;
}

template <>
ValueConverter FixedLengthColumnDictionaryReader<DataTypeDateTime64, DateTime64>::getConverter()
{
    const DataTypeDateTime64 * type = static_cast<const DataTypeDateTime64 *>(data_type.get());
    switch (type->getScale())
    {
        case 0:
            return ValueConverterImpl<DateTime64, 0>::convert;
        case 3:
            return ValueConverterImpl<DateTime64, 3>::convert;
        case 6:
            return ValueConverterImpl<DateTime64, 6>::convert;
        case 9:
            return ValueConverterImpl<DateTime64, 9>::convert;
        default:
            throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported scale {}", type->getScale());
    }
}

template <>
ValueConverter FixedLengthColumnDictionaryReader<DataTypeInt64, Int64>::getConverter()
{
    return ValueConverterImpl<DateTime64, 0>::convert;
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
    if (!rows_to_read)
        return;
    size_t count = idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(rows_to_read));
    chassert(count == rows_to_read);
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
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
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
                plain_decoder->decodeFixedLengthDataSpace(data, row_set, null_map, rows_can_read, element_size, getConverter());
            }
        }
        else
        {
            nextIdxBatchIfEmpty(rows_can_read - null_count);
            if constexpr (std::is_same_v<DataType, typename DB::DataTypeFixedString>)
            {
                auto * string_column = static_cast<ColumnFixedString *>(column.get());
                dict_decoder->decodeFixedStringSpace(dict, string_column->getChars(), row_set, null_map, rows_can_read, element_size);
            }
            else
            {
                auto * number_column = static_cast<DataType::ColumnType *>(column.get());
                auto & data = number_column->getData();
                dict_decoder->decodeFixedLengthDataSpace(dict, data, row_set, null_map, rows_can_read);
            }
        }
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

template <typename DataType, typename DictValueType>
void FixedLengthColumnDictionaryReader<DataType, DictValueType>::read(
    MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
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
                plain_decoder->decodeFixedLengthData(data, row_set, rows_can_read, element_size, getConverter());
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
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

void OptionalColumnReader::nextBatchNullMapIfNeeded(size_t rows_to_read)
{
    if (!cur_null_map.empty())
        return;
    cur_null_map.resize_fill(rows_to_read, 0);
    cur_null_count = 0;
    const auto & def_levels = child->getDefinitionLevels();
    if (def_levels.empty())
        return;
    size_t start = child->levelsOffset();
    int16_t max_def_level = maxDefinitionLevel();
    int16_t max_rep_level = maxRepetitionLevel();
    size_t read = 0;
    size_t count = 0;
    while (read < rows_to_read)
    {
        auto idx = start + count;
        auto dl = def_levels[idx];
        if (dl < max_def_level - 1)
        {
            // for struct reader, when struct is null, child field would be null.
            if (max_rep_level == parent_rl && has_null && dl >= parent_dl)
            {
                cur_null_map[read] = 1;
                cur_null_count++;
            }
            else
            {
                count++;
                continue;
            }
        }
        if (def_levels[idx] == max_def_level - 1)
        {
            if (has_null)
            {
                cur_null_map[read] = 1;
                cur_null_count++;
            }
            else
            {
                count++;
                continue;
            }
        }
        count++;
        read++;
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
        child->readAndDecodePage();
        auto rows_can_read = std::min(rows_to_read - rows_read, child->availableRows());
        if (!rows_can_read) break;
        auto original_filter_offset = row_set? row_set->getOffset() : 0;
        nextBatchNullMapIfNeeded(rows_can_read);
        if (row_set)
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
        // reset filter offset, child reader may modify filter offset
        if (row_set)
        {
            row_set->setOffset(original_filter_offset);
            row_set->addOffset(rows_can_read);
        }
        rows_read += rows_can_read;
        chassert(nested_column->size() == null_data.size());
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

DataTypePtr OptionalColumnReader::getResultType()
{
    return std::make_shared<DataTypeNullable>(child->getResultType());
}

void OptionalColumnReader::applyLazySkip()
{
    skipPageIfNeed();
    while (state.lazy_skip_rows)
    {
        child->readAndDecodePage();
        auto remain_skipped_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
        if (remain_skipped_rows == state.lazy_skip_rows)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "skip values failed. need skip {} rows, but child reader is exhausted", state.lazy_skip_rows);
        }
        state.lazy_skip_rows = remain_skipped_rows;
    }
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
size_t OptionalColumnReader::levelsOffset() const
{
    return child->levelsOffset();
}

const PaddedPODArray<Int16> & OptionalColumnReader::getDefinitionLevels()
{
    return child->getDefinitionLevels();
}
const PaddedPODArray<Int16> & OptionalColumnReader::getRepetitionLevels()
{
    return child->getRepetitionLevels();
}

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
    if (!rows_to_read)
        return;
    state.idx_buffer.resize(rows_to_read);
    size_t count [[maybe_unused]] = idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(rows_to_read));
    chassert(count == rows_to_read);
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
            dict_decoder->decodeStringSpace(dict, string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
        }
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

void StringDictionaryReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
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
        if (row_set)
            row_set->addOffset(rows_can_read);
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
DataTypePtr StringDictionaryReader::getResultType()
{
    return std::make_shared<DataTypeString>();
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
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);

        readAndDecodePage();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.data, row_set, null_map, rows_to_read - null_count);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

void StringDirectReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        readAndDecodePage();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        size_t total_size = plain_decoder->calculateStringTotalSize(state.data, row_set, rows_can_read);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_can_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_can_read);
        if (row_set)
            row_set->addOffset(rows_can_read);
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
DataTypePtr StringDirectReader::getResultType()
{
    return std::make_shared<DataTypeString>();
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
                    const String & value = dict[idx_buffer[count++]];
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
                const String & value = dict[idx_buffer[count++]];
                appendString(chars, string_offsets, value);
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
    if (has_set)
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (row_set.value().get(i))
            {
                const String & value = dict[idx_buffer[i]];
                appendString(chars, string_offsets, value);
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows_to_read; i++)
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
    PaddedPODArray<DictValueType> & dict, PaddedPODArray<DictValueType> & data, const OptionalRowSet & row_set, size_t rows_to_read)
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
    size_t rows_to_read,
    size_t element_size)
{
    size_t rows_read = 0;
    size_t count = 0;
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
                auto & value = dict[idx_buffer[count++]];
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
                data.push_back(dict[idx_buffer[count++]]);
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
    offsets.consume(rows_to_read);
}

template <typename T>
void PlainDecoder::decodeFixedLengthData(
    PaddedPODArray<T> & data,
    const OptionalRowSet & row_set,
    const size_t rows_to_read,
    const size_t element_size,
    ValueConverter value_converter)
{
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!row_set || row_set.value().get(i))
        {
            page_data.checkSize(element_size);
            data.resize(data.size() + 1);
            value_converter(page_data.buffer, element_size, reinterpret_cast<uint8_t *>(data.data() + (data.size() - 1)));
            page_data.consume(element_size);
        }
        else
            page_data.checkAndConsume(element_size);
    }
    offsets.consume(rows_to_read);
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
    offsets.consume(rows_to_read);
}

template <typename T, typename S>
static size_t decodeFixedValueSpaceInternal(
    PaddedPODArray<T> & data, const S * start, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
{
    size_t rows_read = 0;
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
                data.push_back(static_cast<T>(start[count++]));
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
                    data.push_back(0);
                else
                    data.push_back(static_cast<T>(start[count++]));
            }
            else
            {
                if (!null_map[rows_read])
                    count++;
            }
            rows_read++;
        }
    }
    return count;
}

void PlainDecoder::decodeBooleanSpace(
    PaddedPODArray<UInt8> & data,
    PaddedPODArray<UInt8> & src,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    size_t rows_to_read)
{
    decodeFixedValueSpaceInternal(data, src.data(), row_set, null_map, rows_to_read);
    offsets.consume(rows_to_read);
}

template <typename T>
void PlainDecoder::decodeFixedLengthDataSpace(
    PaddedPODArray<T> & data,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    const size_t rows_to_read,
    const size_t element_size,
    ValueConverter value_converter)
{
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
                data.resize(data.size() + 1);
                value_converter(page_data.buffer, element_size, reinterpret_cast<uint8_t *>(data.data() + data.size() - 1));
                page_data.consume(element_size);
            }
        }
        else
        {
            if (!null_map[i])
                page_data.checkAndConsume(element_size);
        }
    }
    offsets.consume(rows_to_read);
}

template <typename T, typename S>
void PlainDecoder::decodeFixedValueSpace(
    PaddedPODArray<T> & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
{
    auto count = decodeFixedValueSpaceInternal(data, reinterpret_cast<const S *>(page_data.buffer), row_set, null_map, rows_to_read);
    page_data.checkAndConsume(count * sizeof(S));
    offsets.consume(rows_to_read);
}

static void insertManyToFilter(PaddedPODArray<bool> & filter, bool value, size_t count)
{
    if (!count)
        return;
    filter.resize(filter.size() + count);
    std::fill(filter.end() - count, filter.end(), value);
}

// must read full
void ListColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    ListState state = getListState(column);
    auto & offsets = state.offsets;
    const bool has_filter = row_set.has_value();
    size_t valid_count = has_filter ? row_set->count() : rows_to_read;
    auto target_size = offsets.size() + valid_count;
    offsets.reserve(target_size);

    size_t rows_read = 0;
    // read data from multiple pages
    bool finished = true;
    size_t count = 0;
    size_t last_row_level_idx = 0;
    auto appendRecord = [&](size_t size)
    {
        if (!finished) [[unlikely]]
        {
            offsets.back() += size;
            finished = true;
        }
        else
            offsets.push_back(offsets.back() + size);
        last_row_level_idx = count;
    };
    while (rows_read < rows_to_read || !finished)
    {
        int array_size = 0;
        size_t tail_empty_rows = 0;
        //        size_t old_offsets_size = offsets.size();
        size_t old_max_offset = offsets.back();
        const auto & def_levels = getDefinitionLevels();
        bool has_def_level = !def_levels.empty();
        const auto & rep_levels = getRepetitionLevels();
        size_t min_count = minimumAvailableLevels();
        size_t start = levelsOffset();
        size_t levels_size = rep_levels.size();
        if (levels_size == 0)
            break;

        PaddedPODArray<bool> child_filter;
        if (has_filter)
            child_filter.reserve(rows_to_read);
        // read from current page
        while (true)
        {
            size_t idx = start + count;
            // levels out of range or rows out of range
            if (count >= min_count || idx >= levels_size || (rows_read >= rows_to_read && finished))
                break;
            int16_t rl = rep_levels[idx];
            int16_t dl = has_def_level ? def_levels[idx] : 0;
            if (rl <= rep_level)
            {
                if (last_row_level_idx < count)
                {
                    rows_read += finished;
                    if (has_filter)
                    {
                        auto valid = row_set->get(rows_read - (finished || rows_read));
                        if (valid)
                        {
                            insertManyToFilter(child_filter, true, array_size);
                            appendRecord(array_size);
                        }
                        else
                        {
                            insertManyToFilter(child_filter, false, array_size);
                            last_row_level_idx = count;
                            finished = true;
                        }
                    }
                    else
                        appendRecord(array_size);
                    tail_empty_rows = array_size > 0 ? 0 : tail_empty_rows + 1;
                    array_size = 0;
                    if (rows_read >= rows_to_read && finished)
                        break;
                }

                 if (has_def_level && dl < def_level)
                {
                    // skip empty record in parent level
                    if (rl != rep_level && (!parent || (parent && dl <= parent_dl)))
                    {
                        count++;
                        last_row_level_idx = count;
                        tail_empty_rows ++;
                        continue;
                    }
                    // value is null
                    if (!has_filter || row_set->get(rows_read))
                    {
                        if (state.null_map)
                        {
                            state.null_map->data()[offsets.size()] = 1;
                        }
                        appendRecord(0);
                    }
                    else
                    {
                        last_row_level_idx = count;
                    }
                    tail_empty_rows++;
                    chassert(array_size == 0);
                    count++;
                    continue;
                }
                else
                {
                    array_size += (dl > def_level);
                }
            }
            else
            {
                array_size += (rl == rep_level + 1);
            }
            count++;
        }

        // read tail record
        if ((rows_read < rows_to_read || !finished) && last_row_level_idx < count)
        {
            if (has_filter)
            {
                auto valid = row_set->get(rows_read - (!finished && rows_read));
                rows_read += finished;
                if (valid)
                {
                    insertManyToFilter(child_filter, true, array_size);
                    appendRecord(array_size);
                }
                else
                {
                    insertManyToFilter(child_filter, false, array_size);
                    finished = true;
                    last_row_level_idx = count;
                }
            }
            else
            {
                rows_read += finished;
                appendRecord(array_size);
            }
            tail_empty_rows = array_size > 0 ? 0 : tail_empty_rows + 1;
        }
        OptionalRowSet filter;
        if (has_filter)
            filter = RowSet(child_filter);
        auto need_read = has_filter ? filter->totalRows() : offsets.back() - old_max_offset;
        for (size_t i = 0; i < children.size(); i++)
        {
            auto & child = children[i];
            child->read(state.columns.at(i), filter, need_read);
            chassert(state.columns.at(i)->size() == offsets.back());
            if (child->isLeafReader())
            {
                child->advance(last_row_level_idx - need_read, false);
            }
            if (filter)
                filter->setOffset(0);
        }

        // skip tail empty records, child reader never read tail empty rows
        if (tail_empty_rows)
            for (auto & child : children)
            {
                if (!child->isLeafReader())
                {
                    child->advance(tail_empty_rows, true);
                }
            }

        // check last row finished
        const auto & next_rep_levels = getRepetitionLevels();
        if (last_row_level_idx < min_count || next_rep_levels.empty() || next_rep_levels[levelsOffset()] <= rep_level) [[likely]]
            finished = true;
        else
            finished = false;
        count = 0;
        last_row_level_idx = 0;
    }
}

void ListColumnReader::computeRowSet(std::optional<RowSet> &, size_t)
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}
MutableColumnPtr ListColumnReader::createColumn()
{
    return ColumnArray::create(children.front()->createColumn(), ColumnArray::ColumnOffsets::create());
}
void ListColumnReader::skip(size_t rows)
{
    // may be can skip generate columns.
    auto tmp = createColumn();
    OptionalRowSet set = RowSet(rows);
    set->setAllFalse();
    read(tmp, set, rows);
}

size_t ListColumnReader::availableRows() const
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}
DataTypePtr ListColumnReader::getResultType()
{
    return std::make_shared<DataTypeArray>(children.front()->getResultType());
}
const PaddedPODArray<Int16> & ListColumnReader::getDefinitionLevels()
{
    return children.front()->getDefinitionLevels();
}
const PaddedPODArray<Int16> & ListColumnReader::getRepetitionLevels()
{
    return children.front()->getRepetitionLevels();
}
size_t ListColumnReader::levelsOffset() const
{
    return children.front()->levelsOffset();
}
ListColumnReader::ListState ListColumnReader::getListState(MutableColumnPtr & column)
{
    // support list inside nullable ?
    NullMap * null_map = nullptr;
    MutableColumnPtr & nested_column = column;
    if (column->isNullable())
    {
        ColumnNullable * null_column = static_cast<ColumnNullable *>(column.get());
        null_map = &null_column->getNullMapData();
        nested_column = null_column->getNestedColumnPtr()->assumeMutable();
    }
    if (!checkColumn<ColumnArray>(*nested_column))
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "column type should be array, but is {}", nested_column->getName());
    }
    ColumnArray * array_column = static_cast<ColumnArray *>(nested_column.get());

    auto & offsets = array_column->getOffsets();
    MutableColumns data_columns;
    data_columns.push_back(array_column->getDataPtr()->assumeMutable());
    return ListState{.null_map = null_map, .offsets = offsets, .columns = std::move(data_columns)};
}

MutableColumnPtr MapColumnReader::createColumn()
{
    MutableColumns columns;
    for (auto & child : children)
    {
        columns.push_back(child->createColumn());
    }
    MutableColumnPtr tuple = ColumnTuple::create(std::move(columns));
    MutableColumnPtr array = ColumnArray::create(std::move(tuple));
    return ColumnMap::create(std::move(array));
}

DataTypePtr MapColumnReader::getResultType()
{
    DataTypes types = {children.front()->getResultType(), children.back()->getResultType()};
    return std::make_shared<DataTypeMap>(std::move(types));
}
void MapColumnReader::advance(size_t rows, bool force)
{
    children.front()->advance(rows, force);
    if (children.back()->isLeafReader() || force)
        children.back()->advance(rows, force);
}
size_t MapColumnReader::minimumAvailableLevels()
{
    return std::min(children.front()->minimumAvailableLevels(), children.back()->minimumAvailableLevels());
}
ListColumnReader::ListState MapColumnReader::getListState(MutableColumnPtr & column)
{
    // support map inside nullable, how parquet serialize null in map type?
    NullMap * null_map = nullptr;
    MutableColumnPtr & column_inside_nullable = column;
    if (column->isNullable())
    {
        ColumnNullable * null_column = static_cast<ColumnNullable *>(column.get());
        null_map = &null_column->getNullMapData();
        column_inside_nullable = null_column->getNestedColumnPtr()->assumeMutable();
    }
    if (!checkColumn<ColumnMap>(*column_inside_nullable))
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "column type should be map, but is {}", column_inside_nullable->getName());
    }
    ColumnMap * map_column = static_cast<ColumnMap *>(column_inside_nullable.get());
    ColumnArray * array_in_map_column = &map_column->getNestedColumn();
    auto data_column = array_in_map_column->getDataPtr();
    const ColumnTuple & tuple_col = checkAndGetColumn<ColumnTuple>(*data_column);
    if (tuple_col.getColumns().size() != 2)
    {
        throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "map column should have 2 columns, but has {}", tuple_col.getColumns().size());
    }
    MutableColumns data_columns;
    for (const auto & col : tuple_col.getColumns())
    {
        data_columns.push_back(col->assumeMutable());
    }
    auto & offsets = array_in_map_column->getOffsets();
    return {null_map, offsets, std::move(data_columns)};
}

void StructColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    /// TODO support tuple inside nullable
    checkColumn<ColumnTuple>(*column);
    ColumnTuple * tuple_column = static_cast<ColumnTuple *>(column.get());
    const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(structType.get());
    auto names = tuple_type->getElementNames();
    auto original_offset = row_set ? row_set->getOffset() : 0;
    for (size_t i = 0; i < names.size(); i++)
    {
        auto nested_column = tuple_column->getColumn(i).assumeMutable();
        auto & nested_reader = children.at(names.at(i));
        nested_reader->read(nested_column, row_set, rows_to_read);
        if (row_set)
            row_set->setOffset(original_offset);
    }
}

void StructColumnReader::computeRowSet(std::optional<RowSet> &, size_t)
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}

MutableColumnPtr StructColumnReader::createColumn()
{
    MutableColumns columns;
    const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(structType.get());
    for (const auto & name : tuple_type->getElementNames())
    {
        auto & nested_reader = children.at(name);
        columns.push_back(nested_reader->createColumn());
    }
    return ColumnTuple::create(std::move(columns));
}

const PaddedPODArray<Int16> & StructColumnReader::getDefinitionLevels()
{
    return children.begin()->second->getDefinitionLevels();
}
const PaddedPODArray<Int16> & StructColumnReader::getRepetitionLevels()
{
    return children.begin()->second->getRepetitionLevels();
}

void StructColumnReader::skip(size_t rows)
{
    for (auto & child : children)
    {
        child.second->skip(rows);
    }
}

size_t StructColumnReader::availableRows() const
{
    throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported operation");
}

size_t StructColumnReader::levelsOffset() const
{
    return children.begin()->second->levelsOffset();
}

DataTypePtr StructColumnReader::getResultType()
{
    return structType;
}
void StructColumnReader::advance(size_t rows, bool force)
{
    for (auto & child : children)
    {
        if (child.second->isLeafReader() || force)
            child.second->advance(rows, force);
    }
}
size_t StructColumnReader::minimumAvailableLevels()
{
    size_t min_levels = std::numeric_limits<size_t>::max();
    for (auto & child : children)
    {
        min_levels = std::min(min_levels, child.second->minimumAvailableLevels());
    }
    return min_levels;
}

BooleanColumnReader::BooleanColumnReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_)
    : SelectiveColumnReader(page_reader_creator_, scan_spec_)
{
}
MutableColumnPtr BooleanColumnReader::createColumn()
{
    return ColumnUInt8::create();
}

void BooleanColumnReader::initBitReader()
{
    if (!state.data.buffer)
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "page buffer is not initialized");
    }
    if (bit_reader && bit_reader->bytes_left() > 0)
        return;
    bit_reader = std::make_unique<arrow::bit_util::BitReader>(state.data.buffer, state.data.buffer_size);
}

void BooleanColumnReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    if (!buffer.empty())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "buffer is not empty");
    }
    if (!row_set)
        return;
    readAndDecodePage();
    initBitReader();

    buffer.resize(rows_to_read);
    size_t count = bit_reader->GetBatch(1, buffer.data(), static_cast<int>(rows_to_read));
    chassert(count == rows_to_read);
    computeRowSetPlain(buffer.data(), row_set, scan_spec.filter, rows_to_read);
}

void BooleanColumnReader::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8, 4096> & null_map, size_t null_count, size_t rows_to_read)
{
    if (!buffer.empty())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "buffer is not empty");
    }
    if (!row_set)
        return;
    readAndDecodePage();
    initBitReader();

    buffer.resize(rows_to_read - null_count);
    size_t count [[maybe_unused]] = bit_reader->GetBatch(1, buffer.data(), static_cast<int>(rows_to_read - null_count));
    chassert(count == rows_to_read - null_count);
    computeRowSetPlainSpace(buffer.data(), row_set, scan_spec.filter, null_map, rows_to_read);
}

void BooleanColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    if (!buffer.empty())
    {
        chassert(rows_to_read == buffer.size());
        ColumnUInt8 * uint8_col = static_cast<ColumnUInt8 *>(column.get());
        auto & data = uint8_col->getData();
        plain_decoder->decodeBoolean(data, buffer, row_set, rows_to_read);
    }
    else
    {
        size_t rows_read = 0;
        while (rows_read < rows_to_read)
        {
            readAndDecodePage();
            initBitReader();

            auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
            buffer.resize(rows_can_read);
            size_t count [[maybe_unused]] = bit_reader->GetBatch(1, buffer.data(), static_cast<int>(rows_can_read));
            chassert(count == rows_can_read);
            auto * number_column = static_cast<ColumnUInt8 *>(column.get());
            auto & data = number_column->getData();
            plain_decoder->decodeBoolean(data, buffer, row_set, rows_can_read);
            buffer.resize(0);
            if (row_set)
                row_set->addOffset(rows_can_read);
            rows_read += rows_can_read;
        }
    }
}
void BooleanColumnReader::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    if (!buffer.empty())
    {
        chassert(rows_to_read == buffer.size());
        ColumnUInt8 * uint8_col = static_cast<ColumnUInt8 *>(column.get());
        auto & data = uint8_col->getData();
        plain_decoder->decodeBooleanSpace(data, buffer, row_set, null_map, rows_to_read);
    }
    else
    {
        size_t rows_read = 0;
        while (rows_read < rows_to_read)
        {
            readAndDecodePage();
            initBitReader();

            auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
            buffer.resize(rows_can_read - null_count);
            size_t count [[maybe_unused]] = bit_reader->GetBatch(1, buffer.data(), static_cast<int>(rows_can_read - null_count));
            chassert(count == rows_can_read - null_count);
            auto * number_column = static_cast<ColumnUInt8 *>(column.get());
            auto & data = number_column->getData();
            plain_decoder->decodeBooleanSpace(data, buffer, row_set, null_map, rows_can_read);
            buffer.resize(0);
            if (row_set)
                row_set->addOffset(rows_can_read);
            rows_read += rows_can_read;
        }
    }
    buffer.clear();
}
size_t BooleanColumnReader::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);

    if (!buffer.empty())
    {
        // only support skip all
        chassert(buffer.size() == skipped);
        buffer.clear();
    }
    else
    {
        initBitReader();
        if (!bit_reader->Advance(skipped))
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "skip rows failed. don't have enough data");
        state.idx_buffer.clear();
    }
    return rows_to_skip - skipped;
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
template class NumberColumnDirectReader<DataTypeDecimal32, Int32>;
template class NumberColumnDirectReader<DataTypeDecimal64, Int64>;
template class FixedLengthColumnDirectReader<DataTypeFixedString>;
template class FixedLengthColumnDirectReader<DataTypeDecimal32>;
template class FixedLengthColumnDirectReader<DataTypeDecimal64>;
template class FixedLengthColumnDirectReader<DataTypeDecimal128>;
template class FixedLengthColumnDirectReader<DataTypeDecimal256>;
// read from int96
template class FixedLengthColumnDirectReader<DataTypeDateTime64>;
template class FixedLengthColumnDirectReader<DataTypeInt64>;


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
template class NumberDictionaryReader<DataTypeDecimal32, Int32>;
template class NumberDictionaryReader<DataTypeDecimal64, Int64>;
template class FixedLengthColumnDictionaryReader<DataTypeFixedString, String>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal32, Decimal32>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal64, Decimal64>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal128, Decimal128>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal256, Decimal256>;
// read from int96
template class FixedLengthColumnDictionaryReader<DataTypeDateTime64, DateTime64>;
template class FixedLengthColumnDictionaryReader<DataTypeInt64, Int64>;

}
