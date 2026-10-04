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

    static void reverseBytes(const UInt8 * src, UInt8 * dst, size_t size)
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
        /// Size the result from the number of rows we are asked to produce, not from the backing buffers:
        /// the source column may be larger than `input_rows_count` during partial evaluation.
        /// Allocate the large `res_data` before the small `res_offsets`. In the other order the small allocation
        /// can take the start of the memory freed by the previous block, so the chars no longer fit there and go to
        /// fresh pages: the performance tests showed a third more soft page faults and a quarter slower `reverse`
        /// of mixed-length strings, all of it in system time.
        res_data.resize_exact(input_rows_count ? offsets[input_rows_count - 1] : 0);
        res_offsets.assign(offsets.begin(), offsets.begin() + input_rows_count);

        /// Load the base pointers once. Going through the columns inside the loop makes the compiler
        /// reload them for every row, because the byte stores below may alias the column objects themselves.
        const UInt8 * src = data.data();
        UInt8 * dst = res_data.data();
        const ColumnString::Offset * offsets_data = offsets.data();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const ColumnString::Offset next_offset = offsets_data[i];
            const size_t size = next_offset - prev_offset;

            /// The long path first: it is the common one for real data, so it gets the shortest branch chain.
            if (size >= max_word_path_size)
                reverseBytes(src + prev_offset, dst + prev_offset, size);
            else if (size == sizeof(UInt64))
                reverseBytes8(src + prev_offset, dst + prev_offset);
            else
                reverseBytesByWords(src + prev_offset, dst + prev_offset, size);

            prev_offset = next_offset;
        }
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t n, ColumnString::Chars & res_data, size_t input_rows_count)
    {
        res_data.resize_exact(input_rows_count * n);

        const UInt8 * src = data.data();
        UInt8 * dst = res_data.data();

        if (n == 1)
        {
            memcpy(dst, src, input_rows_count);
            return;
        }

        if (n == sizeof(UInt64))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t offset = i * n;
                reverseBytes8(src + offset, dst + offset);
            }
            return;
        }

        if (n < max_word_path_size)
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t offset = i * n;
                reverseBytesByWords(src + offset, dst + offset, n);
            }
            return;
        }

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t offset = i * n;
            reverseBytes(src + offset, dst + offset, n);
        }
    }
};

}
