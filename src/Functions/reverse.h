#pragma once

#include <Columns/ColumnString.h>

namespace DB
{

/** Reverse the string as a sequence of bytes.
  */
struct ReverseImpl
{
    static void reverseBytes(const UInt8 * __restrict src, UInt8 * __restrict dst, size_t size)
    {
        for (size_t i = 0; i < size; ++i)
            dst[i] = src[size - i - 1];
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
            ColumnString::Offset next_offset = offsets[i];
            const size_t size = next_offset - prev_offset;
            if (size)
                reverseBytes(data.data() + prev_offset, res_data.data() + prev_offset, size);
            prev_offset = next_offset;
        }
    }

    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t n,
        ColumnString::Chars & res_data,
        size_t input_rows_count)
    {
        res_data.resize_exact(data.size());

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const size_t offset = i * n;
            reverseBytes(data.data() + offset, res_data.data() + offset, n);
        }
    }
};

}
