#pragma once
#include <bit>
#include <iostream>
#include <vector>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <arrow/util/decimal.h>
#include <arrow/util/rle_encoding.h>
#include <parquet/page_index.h>
#include <Common/PODArray.h>

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
}

namespace DB
{

struct ParquetData
{
    // raw page data
    const uint8_t * buffer = nullptr;
    // size of raw page data
    size_t buffer_size = 0;

    void checkSize(size_t size) const
    {
        if (size > buffer_size) [[unlikely]]
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "ParquetData: buffer size is not enough, {} > {}", size, buffer_size);
    }

    // before consume, should check size first
    void consume(size_t size)
    {
        buffer += size;
        buffer_size -= size;
    }

    void checkAndConsume(size_t size)
    {
        checkSize(size);
        consume(size);
    }
};

struct PageOffsets
{
    size_t remain_rows = 0;
    size_t levels_offset = 0;

    void consume(size_t rows)
    {
        if (!rows)
            return;
        if (rows > remain_rows)
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "read too many rows: {} > {}", rows, remain_rows);
        }
        remain_rows -= rows;
        levels_offset += rows;
    }

    void reset(size_t rows)
    {
        remain_rows = rows;
        levels_offset = 0;
    }
};


using ValueConverter = std::function<void(const uint8_t *, const size_t, uint8_t *)>;

template <typename Type, int datetime_scale = 0>
struct ValueConverterImpl
{
    static void convert(const uint8_t *, const size_t, uint8_t *)
    {
        throw Exception(
            ErrorCodes::PARQUET_EXCEPTION, "Unsupported convert value, type: {}, datetime_scale {}", typeid(Type).name(), datetime_scale);
    }
};

template <int datetime_scale>
struct ValueConverterImpl<DateTime64, datetime_scale>
{
    static void convert(const uint8_t * src, const size_t, uint8_t * dst)
    {
        const parquet::Int96 * tmp = reinterpret_cast<const parquet::Int96 *>(src);
        static const int max_scale_num = 9;
        static const UInt64 pow10[max_scale_num + 1] = {1000000000, 100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10, 1};
        static const UInt64 spd = 60 * 60 * 24;
        static const UInt64 scaled_day[max_scale_num + 1]
            = {spd,
               10 * spd,
               100 * spd,
               1000 * spd,
               10000 * spd,
               100000 * spd,
               1000000 * spd,
               10000000 * spd,
               100000000 * spd,
               1000000000 * spd};

        auto decoded = parquet::DecodeInt96Timestamp(*tmp);

        uint64_t scaled_nano = decoded.nanoseconds / pow10[datetime_scale];
        DateTime64 * result = reinterpret_cast<DateTime64 *>(dst);
        *result = static_cast<Int64>((decoded.days_since_epoch * scaled_day[datetime_scale]) + scaled_nano);
    }
};

/// TODO handle big endian machine
template <>
struct ValueConverterImpl<Decimal32>
{
    static void convert(const uint8_t * src, const size_t, uint8_t * dst)
    {
        *reinterpret_cast<Decimal<Int32> *>(dst) = std::byteswap(*reinterpret_cast<const int32_t *>(src));
    }
};

template <>
struct ValueConverterImpl<Decimal64>
{
    static void convert(const uint8_t * src, const size_t, uint8_t * dst)
    {
        *reinterpret_cast<Decimal<Int64> *>(dst) = std::byteswap(*reinterpret_cast<const int64_t *>(src));
    }
};

template <>
struct ValueConverterImpl<Int64>
{
    static void convert(const uint8_t * src, const size_t, uint8_t * dst) { memcpySmallAllowReadWriteOverflow15(dst, src, 8); }
};

template <>
struct ValueConverterImpl<Decimal128>
{
    static void convert(const uint8_t * src, const size_t len, uint8_t * dst)
    {
        auto status = arrow::Decimal128::FromBigEndian(src, static_cast<int32_t>(len));
        assert(status.ok());
        status.ValueUnsafe().ToBytes(dst);
    }
};

template <>
struct ValueConverterImpl<Decimal256>
{
    static void convert(const uint8_t * src, const size_t len, uint8_t * dst)
    {
        auto status = arrow::Decimal256::FromBigEndian(src, static_cast<int32_t>(len));
        assert(status.ok());
        status.ValueUnsafe().ToBytes(dst);
    }
};


template <typename T, typename S>
size_t decodeFixedValueSpaceInternal(
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

class PlainDecoder
{
public:
    PlainDecoder(ParquetData & data_, PageOffsets & offsets_) : page_data(data_), offsets(offsets_) { }

    template <typename T, typename S>
    void decodeFixedValue(PaddedPODArray<T> & data, const OptionalRowSet & row_set, size_t rows_to_read)
    {
        const S * start = reinterpret_cast<const S *>(page_data.buffer);
        page_data.checkSize(rows_to_read * sizeof(S));
        decodeFixedValueInternal(data, start, row_set, rows_to_read);
        page_data.consume(rows_to_read * sizeof(S));
        offsets.consume(rows_to_read);
    }

    void decodeBoolean(PaddedPODArray<UInt8> & data, PaddedPODArray<UInt8> & src, const OptionalRowSet & row_set, size_t rows_to_read);

    void decodeFixedString(ColumnFixedString::Chars & data, const OptionalRowSet & row_set, size_t rows_to_read, size_t n);

    template <typename T>
    void decodeFixedLengthData(
        PaddedPODArray<T> & data, const OptionalRowSet & row_set, size_t rows_to_read, size_t element_size, ValueConverter value_converter)
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

    void decodeString(ColumnString::Chars & chars, ColumnString::Offsets & offsets, const OptionalRowSet & row_set, size_t rows_to_read);

    template <typename T, typename S>
    void
    decodeFixedValueSpace(PaddedPODArray<T> & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
    {
        auto count = decodeFixedValueSpaceInternal(data, reinterpret_cast<const S *>(page_data.buffer), row_set, null_map, rows_to_read);
        page_data.checkAndConsume(count * sizeof(S));
        offsets.consume(rows_to_read);
    }

    void decodeBooleanSpace(
        PaddedPODArray<UInt8> & data,
        PaddedPODArray<UInt8> & src,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

    void decodeFixedStringSpace(
        ColumnFixedString::Chars & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read, size_t n);

    template <typename T>
    void decodeFixedLengthDataSpace(
        PaddedPODArray<T> & data,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read,
        size_t element_size,
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

    void decodeStringSpace(
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

    size_t calculateStringTotalSize(const ParquetData & data, const OptionalRowSet & row_set, size_t rows_to_read);

    size_t calculateStringTotalSizeSpace(
        const ParquetData & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read);

private:
    void
    addOneString(bool null, const ParquetData & data, size_t & offset, const OptionalRowSet & row_set, size_t row, size_t & total_size);

    ParquetData & page_data;
    PageOffsets & offsets;
};

class DictDecoder
{
public:
    DictDecoder(PaddedPODArray<Int32> & idx_buffer_, PageOffsets & offsets_) : idx_buffer(idx_buffer_), offsets(offsets_) { }

    template <class DictValueType>
    void decodeFixedValue(
        PaddedPODArray<DictValueType> & batch_buffer,
        PaddedPODArray<DictValueType> & data,
        const OptionalRowSet & row_set,
        size_t rows_to_read);

    void
    decodeFixedString(PaddedPODArray<String> & dict, ColumnFixedString::Chars & chars, const OptionalRowSet & row_set, size_t rows_to_read);

    template <class DictValueType>
    void decodeFixedLengthData(
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

    void decodeString(
        std::vector<String> & dict,
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const OptionalRowSet & row_set,
        size_t rows_to_read);

    template <class DictValueType>
    void decodeFixedValueSpace(
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

    void decodeStringSpace(
        std::vector<String> & dict,
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

    void decodeFixedStringSpace(
        PaddedPODArray<String> & dict,
        ColumnFixedString::Chars & chars,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read,
        size_t element_size);

    template <class DictValueType>
    void decodeFixedLengthDataSpace(
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

private:
    PaddedPODArray<Int32> & idx_buffer;
    PageOffsets & offsets;
};

template <typename T, typename S>
void decodeFixedValueInternal(PaddedPODArray<T> & data, const S * start, const OptionalRowSet & row_set, size_t rows_to_read)
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

}
