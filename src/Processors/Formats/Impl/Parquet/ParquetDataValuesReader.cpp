#include "ParquetDataValuesReader.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>

#include <arrow/util/decimal.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PARQUET_EXCEPTION;
}

RleValuesReader::RleValuesReader(
    std::unique_ptr<arrow::bit_util::BitReader> bit_reader_, Int32 bit_width_)
    : bit_reader(std::move(bit_reader_)), bit_width(bit_width_)
{
    if (unlikely(bit_width >= 64))
    {
        // e.g. in GetValue_ in bit_stream_utils.h, uint64 type is used to read bit values
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported bit width {}", bit_width);
    }
}

void RleValuesReader::nextGroup()
{
    // refer to:
    // RleDecoder::NextCounts in rle_encoding.h and VectorizedRleValuesReader::readNextGroup in Spark
    UInt32 indicator_value = 0;
    [[maybe_unused]] auto read_res = bit_reader->GetVlqInt(&indicator_value);
    assert(read_res);

    cur_group_is_packed = indicator_value & 1;
    cur_group_size = indicator_value >> 1;

    if (cur_group_is_packed)
    {
        cur_group_size *= 8;
        cur_packed_bit_values.resize(cur_group_size);
        bit_reader->GetBatch(bit_width, cur_packed_bit_values.data(), cur_group_size);
    }
    else
    {
        cur_value = 0;
        read_res = bit_reader->GetAligned((bit_width + 7) / 8, &cur_value);
        assert(read_res);
    }
    cur_group_cursor = 0;

}

template <typename IndividualVisitor, typename RepeatedVisitor>
void RleValuesReader::visitValues(
    UInt32 num_values, IndividualVisitor && individual_visitor, RepeatedVisitor && repeated_visitor)
{
    // refer to: VisitNullBitmapInline in visitor_inline.h
    while (num_values)
    {
        nextGroupIfNecessary();
        auto cur_count = std::min(num_values, curGroupLeft());

        if (cur_group_is_packed)
        {
            for (auto i = cur_group_cursor; i < cur_group_cursor + cur_count; i++)
            {
                individual_visitor(cur_packed_bit_values[i]);
            }
        }
        else
        {
            repeated_visitor(cur_count, cur_value);
        }
        cur_group_cursor += cur_count;
        num_values -= cur_count;
    }
}

template <typename IndividualVisitor, typename RepeatedVisitor>
void RleValuesReader::visitNullableValues(
    size_t cursor,
    UInt32 num_values,
    Int32 max_def_level,
    LazyNullMap & null_map,
    IndividualVisitor && individual_visitor,
    RepeatedVisitor && repeated_visitor)
{
    while (num_values)
    {
        nextGroupIfNecessary();
        auto cur_count = std::min(num_values, curGroupLeft());

        if (cur_group_is_packed)
        {
            for (auto i = cur_group_cursor; i < cur_group_cursor + cur_count; i++)
            {
                if (cur_packed_bit_values[i] == max_def_level)
                {
                    individual_visitor(cursor);
                }
                else
                {
                    null_map.setNull(cursor);
                }
                cursor++;
            }
        }
        else
        {
            if (cur_value == max_def_level)
            {
                repeated_visitor(cursor, cur_count);
            }
            else
            {
                null_map.setNull(cursor, cur_count);
            }
            cursor += cur_count;
        }
        cur_group_cursor += cur_count;
        num_values -= cur_count;
    }
}

template <typename IndividualNullVisitor, typename SteppedValidVisitor, typename RepeatedVisitor>
void RleValuesReader::visitNullableBySteps(
    size_t cursor,
    UInt32 num_values,
    Int32 max_def_level,
    IndividualNullVisitor && individual_null_visitor,
    SteppedValidVisitor && stepped_valid_visitor,
    RepeatedVisitor && repeated_visitor)
{
    // refer to:
    // RleDecoder::GetBatch in rle_encoding.h and TypedColumnReaderImpl::ReadBatchSpaced in column_reader.cc
    // VectorizedRleValuesReader::readBatchInternal in Spark
    while (num_values > 0)
    {
        nextGroupIfNecessary();
        auto cur_count = std::min(num_values, curGroupLeft());

        if (cur_group_is_packed)
        {
            valid_index_steps.resize(cur_count + 1);
            valid_index_steps[0] = 0;
            auto step_idx = 0;
            auto null_map_cursor = cursor;

            for (auto i = cur_group_cursor; i < cur_group_cursor + cur_count; i++)
            {
                if (cur_packed_bit_values[i] == max_def_level)
                {
                    valid_index_steps[++step_idx] = 1;
                }
                else
                {
                    individual_null_visitor(null_map_cursor);
                    if (unlikely(valid_index_steps[step_idx] == UINT8_MAX))
                    {
                        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "unsupported packed values number");
                    }
                    valid_index_steps[step_idx]++;
                }
                null_map_cursor++;
            }
            valid_index_steps.resize(step_idx + 1);
            stepped_valid_visitor(cursor, valid_index_steps);
        }
        else
        {
            repeated_visitor(cur_value == max_def_level, cursor, cur_count);
        }

        cursor += cur_count;
        cur_group_cursor += cur_count;
        num_values -= cur_count;
    }
}

template <typename TValue, typename ValueGetter>
void RleValuesReader::setValues(TValue * res_values, UInt32 num_values, ValueGetter && val_getter)
{
    visitValues(
        num_values,
        /* individual_visitor */ [&](Int32 val)
        {
            *(res_values++) = val_getter(val);
        },
        /* repeated_visitor */ [&](UInt32 count, Int32 val)
        {
            std::fill(res_values, res_values + count, val_getter(val));
            res_values += count;
        }
    );
}

template <typename TValue, typename ValueGetter>
void RleValuesReader::setValueBySteps(
    TValue * res_values,
    const std::vector<UInt8> & col_data_steps,
    ValueGetter && val_getter)
{
    auto step_iterator = col_data_steps.begin();
    res_values += *(step_iterator++);

    visitValues(
        static_cast<UInt32>(col_data_steps.size() - 1),
        /* individual_visitor */ [&](Int32 val)
        {
            *res_values = val_getter(val);
            res_values += *(step_iterator++);
        },
        /* repeated_visitor */ [&](UInt32 count, Int32 val)
        {
            auto getted_val = val_getter(val);
            for (UInt32 i = 0; i < count; i++)
            {
                *res_values = getted_val;
                res_values += *(step_iterator++);
            }
        }
    );
}


namespace
{

template <typename TColumn, typename TValue = typename TColumn::ValueType>
TValue * getResizedPrimitiveData(TColumn & column, size_t size)
{
    auto old_size = column.size();
    column.getData().resize(size);
    memset(column.getData().data() + old_size, 0, sizeof(TValue) * (size - old_size));
    return column.getData().data();
}

} // anoynomous namespace


template <>
void ParquetPlainValuesReader<ColumnString>::readBatch(
    MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values)
{
    auto & column = *assert_cast<ColumnString *>(col_ptr.get());
    auto cursor = column.size();

    column.getOffsets().resize(cursor + num_values);
    auto * offset_data = column.getOffsets().data();
    auto & chars = column.getChars();

    def_level_reader->visitValues(
        num_values,
        /* individual_visitor */ [&](Int32 val)
        {
            if (val == max_def_level)
            {
                plain_data_buffer.readString(column, cursor);
            }
            else
            {
                chars.push_back(0);
                offset_data[cursor] = chars.size();
                null_map.setNull(cursor);
            }
            cursor++;
        },
        /* repeated_visitor */ [&](UInt32 count, Int32 val)
        {
            if (val == max_def_level)
            {
                for (UInt32 i = 0; i < count; i++)
                {
                    plain_data_buffer.readString(column, cursor);
                    cursor++;
                }
            }
            else
            {
                null_map.setNull(cursor, count);

                auto chars_size_bak = chars.size();
                chars.resize(chars_size_bak + count);
                memset(&chars[chars_size_bak], 0, count);

                auto idx = cursor;
                cursor += count;
                for (auto val_offset = chars_size_bak; idx < cursor; idx++)
                {
                    offset_data[idx] = ++val_offset;
                }
            }
        }
    );
}

template <typename TColumn>
void ParquetBitPlainReader<TColumn>::readBatch(
    MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values)
{
    auto cursor = col_ptr->size();
    auto * column_data = getResizedPrimitiveData(*assert_cast<TColumn *>(col_ptr.get()), cursor + num_values);

    def_level_reader->visitNullableValues(
    cursor,
    num_values,
    max_def_level,
    null_map,
        /* individual_visitor */ [&](size_t nest_cursor)
        {
            uint8_t byte;
            bit_reader->GetValue(1, &byte);
            column_data[nest_cursor] = byte;
        },
        /* repeated_visitor */ [&](size_t nest_cursor, UInt32 count)
        {
            bit_reader->GetBatch(1, &column_data[nest_cursor], count);
        }
    );
}


template <>
void ParquetPlainValuesReader<ColumnDecimal<DateTime64>, ParquetReaderTypes::TimestampInt96>::readBatch(
    MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values)
{
    auto cursor = col_ptr->size();
    auto * column_data = getResizedPrimitiveData(
        *assert_cast<ColumnDecimal<DateTime64> *>(col_ptr.get()), cursor + num_values);

    def_level_reader->visitNullableValues(
        cursor,
        num_values,
        max_def_level,
        null_map,
        /* individual_visitor */ [&](size_t nest_cursor)
        {
            plain_data_buffer.readDateTime64FromInt96(column_data[nest_cursor]);
        },
        /* repeated_visitor */ [&](size_t nest_cursor, UInt32 count)
        {
            auto * col_data_pos = column_data + nest_cursor;
            for (UInt32 i = 0; i < count; i++)
            {
                plain_data_buffer.readDateTime64FromInt96(col_data_pos[i]);
            }
        }
    );
}

template <typename TColumn, ParquetReaderTypes reader_type>
void ParquetPlainValuesReader<TColumn, reader_type>::readBatch(
    MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values)
{
    auto cursor = col_ptr->size();
    auto * column_data = getResizedPrimitiveData(*assert_cast<TColumn *>(col_ptr.get()), cursor + num_values);
    using TValue = std::decay_t<decltype(*column_data)>;

    def_level_reader->visitNullableValues(
        cursor,
        num_values,
        max_def_level,
        null_map,
        /* individual_visitor */ [&](size_t nest_cursor)
        {
            plain_data_buffer.readValue(column_data[nest_cursor]);
        },
        /* repeated_visitor */ [&](size_t nest_cursor, UInt32 count)
        {
            plain_data_buffer.readBytes(column_data + nest_cursor, count * sizeof(TValue));
        }
    );
}


template <typename TColumn>
void ParquetFixedLenPlainReader<TColumn>::readBatch(
    MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values)
{
    if constexpr (std::same_as<TColumn, ColumnDecimal<Decimal128>> || std::same_as<TColumn, ColumnDecimal<Decimal256>>)
    {
        readOverBigDecimal(col_ptr, null_map, num_values);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported type");
    }
}

template <typename TColumnOverBigDecimal>
void ParquetFixedLenPlainReader<TColumnOverBigDecimal>::readOverBigDecimal(
    MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values)
{
    auto cursor = col_ptr->size();
    auto * column_data = getResizedPrimitiveData(
        *assert_cast<TColumnOverBigDecimal *>(col_ptr.get()), cursor + num_values);

    def_level_reader->visitNullableValues(
        cursor,
        num_values,
        max_def_level,
        null_map,
        /* individual_visitor */ [&](size_t nest_cursor)
        {
            plain_data_buffer.readOverBigDecimal(column_data + nest_cursor, elem_bytes_num);
        },
        /* repeated_visitor */ [&](size_t nest_cursor, UInt32 count)
        {
            auto col_data_pos = column_data + nest_cursor;
            for (UInt32 i = 0; i < count; i++)
            {
                plain_data_buffer.readOverBigDecimal(col_data_pos + i, elem_bytes_num);
            }
        }
    );
}


template <typename TColumnVector>
void ParquetRleLCReader<TColumnVector>::readBatch(
    MutableColumnPtr & index_col, LazyNullMap & null_map, UInt32 num_values)
{
    auto cursor = index_col->size();
    auto * column_data = getResizedPrimitiveData(*assert_cast<TColumnVector *>(index_col.get()), cursor + num_values);

    bool has_null = false;

    // in ColumnLowCardinality, first element in dictionary is null
    // so we should increase each value by 1 in parquet index
    auto val_getter = [&](Int32 val) { return val + 1; };

    def_level_reader->visitNullableBySteps(
        cursor,
        num_values,
        max_def_level,
        /* individual_null_visitor */ [&](size_t nest_cursor)
        {
            column_data[nest_cursor] = 0;
            has_null = true;
        },
        /* stepped_valid_visitor */ [&](size_t nest_cursor, const std::vector<UInt8> & valid_index_steps)
        {
            rle_data_reader->setValueBySteps(column_data + nest_cursor, valid_index_steps, val_getter);
        },
        /* repeated_visitor */ [&](bool is_valid, size_t nest_cursor, UInt32 count)
        {
            if (is_valid)
            {
                rle_data_reader->setValues(column_data + nest_cursor, count, val_getter);
            }
            else
            {
                auto data_pos = column_data + nest_cursor;
                std::fill(data_pos, data_pos + count, 0);
                has_null = true;
            }
        }
    );
    if (has_null)
    {
        null_map.setNull(0);
    }
}

template <>
void ParquetRleDictReader<ColumnString>::readBatch(
    MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values)
{
    auto & column = *assert_cast<ColumnString *>(col_ptr.get());
    auto cursor = column.size();
    std::vector<Int32> value_cache;

    const auto & dict_chars = static_cast<const ColumnString &>(page_dictionary).getChars();
    const auto & dict_offsets = static_cast<const ColumnString &>(page_dictionary).getOffsets();

    column.getOffsets().resize(cursor + num_values);
    auto * offset_data = column.getOffsets().data();
    auto & chars = column.getChars();

    auto append_nulls = [&](UInt8 num)
    {
        for (auto limit = cursor + num; cursor < limit; cursor++)
        {
            chars.push_back(0);
            offset_data[cursor] = chars.size();
            null_map.setNull(cursor);
        }
    };

    auto append_string = [&](Int32 dict_idx)
    {
        auto dict_chars_cursor = dict_offsets[dict_idx - 1];
        auto value_len = dict_offsets[dict_idx] - dict_chars_cursor;
        auto chars_cursor = chars.size();
        chars.resize(chars_cursor + value_len);

        memcpySmallAllowReadWriteOverflow15(&chars[chars_cursor], &dict_chars[dict_chars_cursor], value_len);
        offset_data[cursor] = chars.size();
        cursor++;
    };

    auto val_getter = [&](Int32 val) { return val + 1; };

    def_level_reader->visitNullableBySteps(
        cursor,
        num_values,
        max_def_level,
        /* individual_null_visitor */ [&](size_t) {},
        /* stepped_valid_visitor */ [&](size_t, const std::vector<UInt8> & valid_index_steps)
        {
            value_cache.resize(valid_index_steps.size());
            rle_data_reader->setValues(
                value_cache.data() + 1, static_cast<UInt32>(valid_index_steps.size() - 1), val_getter);

            append_nulls(valid_index_steps[0]);
            for (size_t i = 1; i < valid_index_steps.size(); i++)
            {
                append_string(value_cache[i]);
                append_nulls(valid_index_steps[i] - 1);
            }
        },
        /* repeated_visitor */ [&](bool is_valid, size_t, UInt32 count)
        {
            if (is_valid)
            {
                value_cache.resize(count);
                rle_data_reader->setValues(value_cache.data(), count, val_getter);
                for (UInt32 i = 0; i < count; i++)
                {
                    append_string(value_cache[i]);
                }
            }
            else
            {
                append_nulls(count);
            }
        }
    );
}

template <typename TColumnVector>
void ParquetRleDictReader<TColumnVector>::readBatch(
    MutableColumnPtr & col_ptr, LazyNullMap & null_map, UInt32 num_values)
{
    auto cursor = col_ptr->size();
    auto * column_data = getResizedPrimitiveData(*assert_cast<TColumnVector *>(col_ptr.get()), cursor + num_values);
    const auto & dictionary_array = static_cast<const TColumnVector &>(page_dictionary).getData();

    auto val_getter = [&](Int32 val) { return dictionary_array[val]; };
    def_level_reader->visitNullableBySteps(
        cursor,
        num_values,
        max_def_level,
        /* individual_null_visitor */ [&](size_t nest_cursor)
        {
            null_map.setNull(nest_cursor);
        },
        /* stepped_valid_visitor */ [&](size_t nest_cursor, const std::vector<UInt8> & valid_index_steps)
        {
            rle_data_reader->setValueBySteps(column_data + nest_cursor, valid_index_steps, val_getter);
        },
        /* repeated_visitor */ [&](bool is_valid, size_t nest_cursor, UInt32 count)
        {
            if (is_valid)
            {
                rle_data_reader->setValues(column_data + nest_cursor, count, val_getter);
            }
            else
            {
                null_map.setNull(nest_cursor, count);
            }
        }
    );
}


template class ParquetPlainValuesReader<ColumnInt32>;
template class ParquetPlainValuesReader<ColumnUInt32>;
template class ParquetPlainValuesReader<ColumnInt64>;
template class ParquetPlainValuesReader<ColumnUInt64>;
template class ParquetPlainValuesReader<ColumnFloat32>;
template class ParquetPlainValuesReader<ColumnFloat64>;
template class ParquetPlainValuesReader<ColumnDecimal<Decimal32>>;
template class ParquetPlainValuesReader<ColumnDecimal<Decimal64>>;
template class ParquetPlainValuesReader<ColumnDecimal<DateTime64>>;
template class ParquetPlainValuesReader<ColumnString>;
template class ParquetPlainValuesReader<ColumnUInt8>;

template class ParquetBitPlainReader<ColumnUInt8>;

template class ParquetFixedLenPlainReader<ColumnDecimal<Decimal128>>;
template class ParquetFixedLenPlainReader<ColumnDecimal<Decimal256>>;

template class ParquetRleLCReader<ColumnUInt8>;
template class ParquetRleLCReader<ColumnUInt16>;
template class ParquetRleLCReader<ColumnUInt32>;

template class ParquetRleDictReader<ColumnUInt8>;
template class ParquetRleDictReader<ColumnInt32>;
template class ParquetRleDictReader<ColumnUInt32>;
template class ParquetRleDictReader<ColumnInt64>;
template class ParquetRleDictReader<ColumnUInt64>;
template class ParquetRleDictReader<ColumnFloat32>;
template class ParquetRleDictReader<ColumnFloat64>;
template class ParquetRleDictReader<ColumnDecimal<Decimal32>>;
template class ParquetRleDictReader<ColumnDecimal<Decimal64>>;
template class ParquetRleDictReader<ColumnDecimal<Decimal128>>;
template class ParquetRleDictReader<ColumnDecimal<Decimal256>>;
template class ParquetRleDictReader<ColumnDecimal<DateTime64>>;
template class ParquetRleDictReader<ColumnString>;

}
