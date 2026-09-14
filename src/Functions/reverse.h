#pragma once

#include <bit>
#include <cstring>

#include <Columns/ColumnString.h>
#include <base/unaligned.h>

namespace DB
{

/** Reverse the string as a sequence of bytes.
  */
struct ReverseImpl
{
    static constexpr size_t max_word_path_size = 4 * sizeof(UInt64);

    static void reverseBytes(const UInt8 * __restrict src, UInt8 * __restrict dst, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
            dst[i] = src[size - i - 1];
    }

    template <typename T>
    static void reverseBytesWord(const UInt8 * src, UInt8 * dst)
    {
        const T value = std::byteswap(unalignedLoad<T>(src));
        unalignedStore<T>(dst, value);
    }

    static void reverseBytes8(const UInt8 * src, UInt8 * dst) { reverseBytesWord<UInt64>(src, dst); }

    /// Reverse short strings with word-sized operations. If the size is not a multiple of
    /// the word size, the first and last words overlap. The overlapping bytes are identical,
    /// so this avoids a scalar loop without reading or writing outside the current string.
    static void reverseBytesByWords(const UInt8 * src, UInt8 * dst, size_t size)
    {
        if (size <= sizeof(UInt16))
        {
            if (size == 1)
                dst[0] = src[0];
            else if (size == sizeof(UInt16))
            {
                dst[0] = src[1];
                dst[1] = src[0];
            }

            return;
        }

        if (size < sizeof(UInt64))
        {
            if (size >= sizeof(UInt32))
            {
                reverseBytesWord<UInt32>(src + size - sizeof(UInt32), dst);
                if (size != sizeof(UInt32))
                    reverseBytesWord<UInt32>(src, dst + size - sizeof(UInt32));
            }
            else
            {
                reverseBytesWord<UInt16>(src + size - sizeof(UInt16), dst);
                reverseBytesWord<UInt16>(src, dst + size - sizeof(UInt16));
            }

            return;
        }

        size_t offset = 0;
        for (; offset <= size - sizeof(UInt64); offset += sizeof(UInt64))
            reverseBytes8(src + size - offset - sizeof(UInt64), dst + offset);

        if (offset != size)
            reverseBytes8(src, dst + size - sizeof(UInt64));
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.resize_exact(data.size());
        res_offsets.assign(offsets);

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const ColumnString::Offset next_offset = offsets[i];
            const size_t size = next_offset - prev_offset;

            if (size <= sizeof(UInt16))
            {
                if (size == 1)
                    res_data[prev_offset] = data[prev_offset];
                else if (size == sizeof(UInt16))
                {
                    res_data[prev_offset] = data[next_offset - 1];
                    res_data[prev_offset + 1] = data[prev_offset];
                }
            }
            else if (size == sizeof(UInt64))
                reverseBytes8(data.data() + prev_offset, res_data.data() + prev_offset);
            else if (size < max_word_path_size)
                reverseBytesByWords(data.data() + prev_offset, res_data.data() + prev_offset, size);
            else
                reverseBytes(data.data() + prev_offset, res_data.data() + prev_offset, size);

            prev_offset = next_offset;
        }
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t n, ColumnString::Chars & res_data, size_t input_rows_count)
    {
        res_data.resize_exact(data.size());

        if (n == 1)
        {
            memcpy(res_data.data(), data.data(), input_rows_count);
            return;
        }

        if (n == sizeof(UInt64))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t offset = i * n;
                reverseBytes8(data.data() + offset, res_data.data() + offset);
            }
            return;
        }

        if (n < max_word_path_size)
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t offset = i * n;
                reverseBytesByWords(data.data() + offset, res_data.data() + offset, n);
            }
            return;
        }

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t offset = i * n;
            reverseBytes(data.data() + offset, res_data.data() + offset, n);
        }
    }
};

}
