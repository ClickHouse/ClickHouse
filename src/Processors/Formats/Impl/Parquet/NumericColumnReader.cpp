#include "SelectiveColumnReader.h"

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
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

namespace
{
template <typename T>
concept ColumnIndexType = std::is_base_of_v<parquet::ColumnIndex, T>;

// apply column index to row_set, when result false means all rows are filtered out
template <typename ColumnIndexType>
bool applyColumnIndex(
    OptionalRowSet & row_set,
    ColumnFilterPtr filter,
    Int32 page_position,
    std::shared_ptr<parquet::ColumnIndex> column_index,
    bool has_null)
{
    if (!row_set || !filter)
        return false;
    if (!column_index) return true;
    ColumnIndexType * index = dynamic_cast<ColumnIndexType *>(column_index.get());
    if (!index)
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "column index cast to {} failed", typeid(ColumnIndexType).name());
    chassert(index->min_values().size() > static_cast<size_t>(page_position));
    chassert(index->max_values().size() > static_cast<size_t>(page_position));
    auto min_value = index->min_values()[page_position];
    auto max_value = index->max_values()[page_position];
    bool range_test;
    if constexpr (std::is_same_v<ColumnIndexType, parquet::Int32ColumnIndex> || std::is_same_v<ColumnIndexType, parquet::Int64ColumnIndex>)
        range_test = filter->testInt64Range(min_value, max_value, has_null);
    else if constexpr (std::is_same_v<ColumnIndexType, parquet::FloatColumnIndex>)
        range_test = filter->testFloat32Range(min_value, max_value, has_null);
    else if constexpr (std::is_same_v<ColumnIndexType, parquet::DoubleColumnIndex>)
        range_test = filter->testFloat64Range(min_value, max_value, has_null);
    else
        UNREACHABLE();
    if (!range_test || (has_null && index->null_pages()[page_position] && !filter->testNull()))
    {
        row_set->setAllFalse();
        return false;
    }
    return true;
}
}

template <typename DataType, typename SerializedType>
void NumberColumnDirectReader<DataType, SerializedType>::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    if (!applyColumnIndex<typename IndexTypeTraits<SerializedType>::IndexType>(row_set, scan_spec.filter, state.getCurrentPagePosition(), column_index, false))
        return;
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


template <typename DataType, typename SerializedType>
void NumberColumnDirectReader<DataType, SerializedType>::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    if (!applyColumnIndex<typename IndexTypeTraits<SerializedType>::IndexType>(row_set, scan_spec.filter, state.getCurrentPagePosition(), column_index, true))
        return;
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
    if (!applyColumnIndex<typename IndexTypeTraits<SerializedType>::IndexType>(row_set, scan_spec.filter, state.getCurrentPagePosition(), column_index, false))
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
    if (!applyColumnIndex<typename IndexTypeTraits<SerializedType>::IndexType>(row_set, scan_spec.filter, state.getCurrentPagePosition(), column_index, true))
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
    dict_page_read = true;
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
}
