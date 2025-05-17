#include "SelectiveColumnReader.h"

#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Common/assert_cast.h>

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
extern const int LOGICAL_ERROR;
}

namespace DB
{

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
        readAndDecodePageIfNeeded();
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
        readAndDecodePageIfNeeded();
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
    dict_page_read = true;
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
        readAndDecodePageIfNeeded();
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        if (plain)
        {
            if constexpr (std::is_same_v<DataType, DataTypeFixedString>)
            {
                auto * string_column = static_cast<ColumnFixedString *>(column.get());
                plain_decoder->decodeFixedStringSpace(string_column->getChars(), row_set, null_map, rows_can_read, element_size);
            }
            else
            {
                auto * number_column = static_cast<typename DataType::ColumnType *>(column.get());
                auto & data = number_column->getData();
                plain_decoder->decodeFixedLengthDataSpace(data, row_set, null_map, rows_can_read, element_size, getConverter());
            }
        }
        else
        {
            nextIdxBatchIfEmpty(rows_can_read - null_count);
            if constexpr (std::is_same_v<DataType, DataTypeFixedString>)
            {
                auto * string_column = static_cast<ColumnFixedString *>(column.get());
                dict_decoder->decodeFixedStringSpace(dict, string_column->getChars(), row_set, null_map, rows_can_read, element_size);
            }
            else
            {
                auto * number_column = static_cast<typename DataType::ColumnType *>(column.get());
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
        readAndDecodePageIfNeeded();
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

template class FixedLengthColumnDirectReader<DataTypeFixedString>;
template class FixedLengthColumnDirectReader<DataTypeDecimal32>;
template class FixedLengthColumnDirectReader<DataTypeDecimal64>;
template class FixedLengthColumnDirectReader<DataTypeDecimal128>;
template class FixedLengthColumnDirectReader<DataTypeDecimal256>;
// read from int96
template class FixedLengthColumnDirectReader<DataTypeDateTime64>;
template class FixedLengthColumnDirectReader<DataTypeInt64>;

template class FixedLengthColumnDictionaryReader<DataTypeFixedString, String>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal32, Decimal32>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal64, Decimal64>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal128, Decimal128>;
template class FixedLengthColumnDictionaryReader<DataTypeDecimal256, Decimal256>;
// read from int96
template class FixedLengthColumnDictionaryReader<DataTypeDateTime64, DateTime64>;
template class FixedLengthColumnDictionaryReader<DataTypeInt64, Int64>;
}
