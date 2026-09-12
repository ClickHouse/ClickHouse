#pragma once

#include <Columns/ColumnString.h>

namespace DB
{

/** Reverse the string as a sequence of bytes.
  */
struct ReverseImpl
{
    static NO_INLINE void reverseBytes(
        const UInt8 * __restrict__ src,
        const ColumnString::Offset * __restrict__ offsets,
        UInt8 * __restrict__ dst,
        size_t input_rows_count)
    {
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const ColumnString::Offset next_offset = offsets[i];
            for (size_t j = prev_offset; j < next_offset; ++j)
                dst[j] = src[next_offset + prev_offset - j - 1];
            prev_offset = next_offset;
        }
    }

    static NO_INLINE void reverseBytesFixed(
        const UInt8 * __restrict__ src, UInt8 * __restrict__ dst, size_t n, size_t input_rows_count)
    {
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t offset = i * n;
            for (size_t j = 0; j < n; ++j)
                dst[offset + j] = src[offset + n - j - 1];
        }
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

        reverseBytes(data.data(), offsets.data(), res_data.data(), input_rows_count);
    }

    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t n,
        ColumnString::Chars & res_data,
        size_t input_rows_count)
    {
        res_data.resize_exact(data.size());

        reverseBytesFixed(data.data(), res_data.data(), n, input_rows_count);
    }
};

}
