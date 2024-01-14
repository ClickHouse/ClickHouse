#pragma once

#include <Core/Types.h>

#include <arrow/util/bit_stream_utils.h>
#include <arrow/util/decimal.h>
#include <parquet/types.h>

namespace DB
{

template <typename T> struct ToArrowDecimal;

template <> struct ToArrowDecimal<Decimal<wide::integer<128, signed>>>
{
    using ArrowDecimal = arrow::Decimal128;
};

template <> struct ToArrowDecimal<Decimal<wide::integer<256, signed>>>
{
    using ArrowDecimal = arrow::Decimal256;
};


class ParquetDataBuffer
{
private:

public:
    ParquetDataBuffer(const uint8_t * data_, UInt64 avaible_, UInt8 datetime64_scale_ = DataTypeDateTime64::default_scale)
        : data(reinterpret_cast<const Int8 *>(data_)), avaible(avaible_), datetime64_scale(datetime64_scale_) {}

    template <typename TValue>
    void ALWAYS_INLINE readValue(TValue & dst)
    {
        checkAvaible(sizeof(TValue));
        dst = *reinterpret_cast<const TValue *>(data);
        consume(sizeof(TValue));
    }

    void ALWAYS_INLINE readBytes(void * dst, size_t bytes)
    {
        checkAvaible(bytes);
        memcpy(dst, data, bytes);
        consume(bytes);
    }

    void ALWAYS_INLINE readDateTime64(DateTime64 & dst)
    {
        static const int max_scale_num = 9;
        static const UInt64 pow10[max_scale_num + 1]
            = {1000000000, 100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10, 1};
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

        checkAvaible(sizeof(parquet::Int96));
        auto decoded = parquet::DecodeInt96Timestamp(*reinterpret_cast<const parquet::Int96 *>(data));

        uint64_t scaled_nano = decoded.nanoseconds / pow10[datetime64_scale];
        dst = static_cast<Int64>(decoded.days_since_epoch * scaled_day[datetime64_scale] + scaled_nano);

        consume(sizeof(parquet::Int96));
    }

    /**
     * This method should only be used to read string whose elements size is small.
     * Because memcpySmallAllowReadWriteOverflow15 instead of memcpy is used according to ColumnString::indexImpl
     */
    void ALWAYS_INLINE readString(ColumnString & column, size_t cursor)
    {
        // refer to: PlainByteArrayDecoder::DecodeArrowDense in encoding.cc
        //           deserializeBinarySSE2 in SerializationString.cpp
        checkAvaible(4);
        auto value_len = ::arrow::util::SafeLoadAs<Int32>(getArrowData());
        if (unlikely(value_len < 0 || value_len > INT32_MAX - 4))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid or corrupted value_len '{}'", value_len);
        }
        consume(4);
        checkAvaible(value_len);

        auto chars_cursor = column.getChars().size();
        column.getChars().resize(chars_cursor + value_len + 1);

        memcpySmallAllowReadWriteOverflow15(&column.getChars()[chars_cursor], data, value_len);
        column.getChars().back() = 0;

        column.getOffsets().data()[cursor] = column.getChars().size();
        consume(value_len);
    }

    template <is_over_big_decimal TDecimal>
    void ALWAYS_INLINE readOverBigDecimal(TDecimal * out, Int32 elem_bytes_num)
    {
        using TArrowDecimal = typename ToArrowDecimal<TDecimal>::ArrowDecimal;

        checkAvaible(elem_bytes_num);

        // refer to: RawBytesToDecimalBytes in reader_internal.cc, Decimal128::FromBigEndian in decimal.cc
        auto status = TArrowDecimal::FromBigEndian(getArrowData(), elem_bytes_num);
        if (unlikely(!status.ok()))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Read parquet decimal failed: {}", status.status().ToString());
        }
        status.ValueUnsafe().ToBytes(reinterpret_cast<uint8_t *>(out));
        consume(elem_bytes_num);
    }

private:
    const Int8 * data;
    UInt64 avaible;
    const UInt8 datetime64_scale;

    void ALWAYS_INLINE checkAvaible(UInt64 num)
    {
        if (unlikely(avaible < num))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Consuming {} bytes while {} avaible", num, avaible);
        }
    }

    const uint8_t * ALWAYS_INLINE getArrowData() { return reinterpret_cast<const uint8_t *>(data); }

    void ALWAYS_INLINE consume(UInt64 num)
    {
        data += num;
        avaible -= num;
    }
};


class LazyNullMap
{
public:
    LazyNullMap(UInt64 size_) : size(size_), col_nullable(nullptr) {}

    template <typename T>
    requires std::is_integral_v<T>
    void setNull(T cursor)
    {
        initialize();
        null_map[cursor] = 1;
    }

    template <typename T>
    requires std::is_integral_v<T>
    void setNull(T cursor, UInt32 count)
    {
        initialize();
        memset(null_map + cursor, 1, count);
    }

    ColumnPtr getNullableCol() { return col_nullable; }

private:
    UInt64 size;
    UInt8 * null_map;
    ColumnPtr col_nullable;

    void initialize()
    {
        if (likely(col_nullable))
        {
            return;
        }
        auto col = ColumnVector<UInt8>::create(size);
        null_map = col->getData().data();
        col_nullable = std::move(col);
        memset(null_map, 0, size);
    }
};

}
