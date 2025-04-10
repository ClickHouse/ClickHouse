#pragma once

#include <Processors/Formats/Impl/Parquet/ReadCommon.h>

#include <IO/VarInt.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

//TODO: move implementations to .cpp, including templates
namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_DECOMPRESS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
}

namespace DB::Parquet
{

//TODO:
// * BIT_PACKED for rep/def levels
// * PLAIN BOOLEAN
// * RLE BOOLEAN
// * DELTA_BINARY_PACKED (INT32, INT64)
// * DELTA_LENGTH_BYTE_ARRAY
// * DELTA_BYTE_ARRAY
// * BYTE_STREAM_SPLIT
struct ValueDecoder
{
    /// If canReadDirectlyIntoColumn returns true, decodePage is not called.
    virtual bool canReadDirectlyIntoColumn(parq::Encoding::type, size_t /*num_values*/, IColumn &, char ** /*out_ptr*/, size_t * /*out_bytes*/) const { return false; }
    /// Caller ensures that `data` and `filter` are padded, i.e. have at least PADDING_FOR_SIMD bytes
    /// of readable memory before start and after end.
    /// `num_values_out` is the number of ones in filter[0..num_values_in]. If filter is nullptr,
    /// num_values_out == num_values_in.
    virtual void decodePage(parq::Encoding::type, const char * /*data*/, size_t /*bytes*/, size_t /*num_values_in*/, size_t /*num_values_out*/, IColumn &, const UInt8 * /*filter*/) const { chassert(false); }

    virtual ~ValueDecoder() = default;
};


/// ClickHouse's in-memory column data format matches Parquet's PLAIN encoding format.
/// We can directly memcpy/decompress into the column if Encoding == PLAIN.
struct FixedSizeValueDecoder : public ValueDecoder
{
    size_t value_size;

    explicit FixedSizeValueDecoder(size_t value_size_) : value_size(value_size_) {}

    bool canReadDirectlyIntoColumn(parq::Encoding::type encoding, size_t num_values, IColumn & col, char ** out_ptr, size_t * out_bytes) const override
    {
        chassert(col.sizeOfValueIfFixed() == value_size);
        if (encoding == parq::Encoding::PLAIN)
        {
            const auto span = col.insertRawUninitialized(num_values);
            *out_ptr = span.data();
            *out_bytes = span.size();
            return true;
        }
        return false;
    }

    void decodePage(parq::Encoding::type encoding, const char * /*data*/, size_t /*bytes*/, size_t /*num_values_in*/, size_t /*num_values_out*/, IColumn &, const UInt8 * /*filter*/) const override
    {
        //TODO (PLAIN, DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Encoding {} for fixed-size types not implemented", thriftToString(encoding));
    }
};

/// Reads INT32 and converts to INT16/INT8. Signed vs unsigned makes no difference.
template <typename T>
struct ShortIntDecoder : public ValueDecoder
{
    void decodePage(parq::Encoding::type encoding, const char * data, size_t bytes, size_t num_values_in, size_t num_values_out, IColumn & col, const UInt8 * filter) const override
    {
        if (encoding == parq::Encoding::PLAIN)
        {
            if (num_values_in * 4 > bytes)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Page data too short");
            const auto span = col.insertRawUninitialized(num_values_out);
            /// `col` may be signed or unsigned, but we cast it to unsigned here (to avoid pointless
            /// extra template instantiations). AFAIU, this is not UB because we go through char*.
            T * out = reinterpret_cast<T *>(span.data());
            chassert(span.size() == num_values_out * sizeof(T));
            if (!filter)
            {
                chassert(num_values_in == num_values_out);
                for (size_t i = 0; i < num_values_in; ++i)
                {
                    /// Read the low bytes of an unaligned 4-byte value.
                    T x;
                    memcpy(&x, data + i * 4, sizeof(T));
                    out[i] = x;
                }
            }
            else
            {
                size_t out_i = 0;
                for (size_t i = 0; i < num_values_in; ++i)
                {
                    if (filter[i])
                    {
                        T x;
                        memcpy(&x, data + i * 4, sizeof(T));
                        out[out_i++] = x;
                    }
                }
            }
        }
        else if (encoding == parq::Encoding::DELTA_BINARY_PACKED)
        {
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_BINARY_PACKED for 16/8-bit ints is not implemented");
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected integer encoding: {}", thriftToString(encoding));
        }
    }
};

/// Used for dictionary indices and repetition/definition levels.
/// For dictionary indices, we add 2 to each value to make it compatible with ColumnLowCardinality,
/// which reserves the first two dictionary slots for NULL and default value.
/// Input, output, and filter must all be padded - we may read/write up to 7 elements past the end.
template <typename T, UInt8 ADD, bool FILTERED>
void decodeBitPackedRLE(size_t limit, const size_t bit_width, size_t num_values_in, const char * data, size_t bytes, const UInt8 * filter, T * out)
{
    static_assert(sizeof(T) <= 4, "");
    chassert(bit_width > 0 && bit_width <= 32);
    chassert(limit + ADD <= size_t(std::numeric_limits<T>::max()) + 1);
    const size_t byte_width = (bit_width + 7) / 8;
    const UInt32 value_mask = (1ul << bit_width) - 1;
    const char * end = data + bytes;
    size_t idx = 0;
    size_t filter_idx = 0;
    /// (Some stats from hits.parquet, in case it helps with optimization:
    ///  bit-packed runs: 64879089, total 2548822304 values (~39 values/run),
    ///  RLE runs: 81177527, total 7373423915 values (~91 values/run).)
    while (data < end)
    {
        UInt64 len;
        data = readVarUInt(len, data, end - data);
        if (len & 1)
        {
            /// Bit-packed run.
            size_t groups = len >> 1;
            len = groups << 3;
            size_t nbytes = groups * bit_width;
            if (len > num_values_in - idx + 7 || nbytes > size_t(end - data))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Too many RLE-encoded values (bp)");

            /// TODO: For bit_width <= 8, this can probably be made much faster by unrolling 8
            ///       iterations of the loop and using pdep instruction (on x86).
            /// TODO: May make sense to have specialized versions of this loop for some specific
            ///       values of bit_width. E.g. bit_width=1 is very common as def levels for nullables.
            for (size_t bit_idx = 0; bit_idx < (nbytes << 3); bit_idx += bit_width)
            {
                if constexpr (FILTERED)
                {
                    if (!filter[filter_idx++])
                        continue;
                }

                size_t x;
                memcpy(&x, data + (bit_idx >> 3), 8);
                x = (x >> (bit_idx & 7)) & value_mask;

                if (x >= limit)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Dict index or rep/def level out of bounds (bp)");
                x += ADD;
                out[idx++] = x;
            }
            data += nbytes;
        }
        else
        {
            len >>= 1;
            if (len > num_values_in - idx || byte_width > size_t(end - data))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Too many RLE-encoded values (rle)");

            UInt32 x;
            memcpy(&x, data, 4);
            x &= value_mask;

            if (x >= limit)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Dict index or rep/def level out of bounds (rle)");
            x += ADD;

            for (size_t i = 0; i < len; ++i)
            {
                if constexpr (FILTERED)
                {
                    if (!filter[filter_idx++])
                        continue;
                }

                out[idx++] = x;
            }

            data += byte_width;
        }
    }
    if constexpr (!FILTERED)
        filter_idx = idx;
    if (filter_idx < num_values_in || filter_idx > num_values_in + 7)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected number of RLE-encoded values");
}

template <typename T>
struct DictionaryIndexDecoder : public ValueDecoder
{
    size_t limit = 0; // throw if any value is greater than this

    explicit DictionaryIndexDecoder(size_t limit_) : limit(limit_) {}

    void decodePage(parq::Encoding::type encoding, const char * data, size_t bytes, size_t num_values_in, size_t num_values_out, IColumn & col, const UInt8 * filter) const override
    {
        if (encoding != parq::Encoding::RLE && encoding != parq::Encoding::RLE_DICTIONARY)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-RLE encoding: {}", thriftToString(encoding));

        if (bytes < 1)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Page too short (dict indices bit width)");
        size_t bit_width = *data;
        data += 1;
        bytes -= 1;
        if (bit_width < 1 || bit_width > 32)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid dict indices bit width: {}", bit_width);

        auto & array = assert_cast<ColumnVector<T> &>(col).getData();
        size_t start = array.size();
        array.resize(start + num_values_out);
        if (filter)
            decodeBitPackedRLE<T, /*ADD*/ 2, /*FILTERED*/ true>(
                limit, bit_width, num_values_in, data, bytes, filter, array.data() + start);
        else
            decodeBitPackedRLE<T, /*ADD*/ 2, /*FILTERED*/ false>(
                limit, bit_width, num_values_in, data, bytes, nullptr, array.data() + start);
    }
};

struct StringDecoder : public ValueDecoder
{
    void decodePage(parq::Encoding::type encoding, const char * data, size_t bytes, size_t num_values_in, size_t /*num_values_out*/, IColumn & col, const UInt8 * filter) const override
    {
        const char * end = data + bytes;
        /// Keep in mind that ColumnString stores a '\0' after each string.
        auto & col_str = assert_cast<ColumnString &>(col);
        if (encoding == parq::Encoding::PLAIN)
        {
            // 4 byte length stored as little endian, followed by bytes.
            col_str.getChars().reserve(col_str.getChars().size() + (bytes - num_values_in * (4 - 1)));
            for (size_t idx = 0; idx < num_values_in; ++idx)
            {
                UInt32 x;
                memcpy(&x, data, 4); /// omitting range check because input is padded
                size_t len = x;
                if (4 + len > size_t(end - data))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Encoded string is out of bounds");
                /// TODO: Try optimizing short memcpy by taking advantage of padding.
                if (!filter || filter[idx])
                    col_str.insertData(data + 4, len);
                data += 4 + len;
            }
        }
        else if (encoding == parq::Encoding::DELTA_LENGTH_BYTE_ARRAY)
        {
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_LENGTH_BYTE_ARRAY not implemented");
        }
        else if (encoding == parq::Encoding::DELTA_BYTE_ARRAY)
        {
            //TODO
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_BYTE_ARRAY not implemented");
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected BYTE_ARRAY encoding: {}", thriftToString(encoding));
    }
};

}
