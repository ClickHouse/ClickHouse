#pragma once

#include <Columns/ColumnString.h>

namespace DB
{

/** Reverse the string as a sequence of bytes.
  */
struct ReverseImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.resize_exact(data.size());
        res_offsets.assign(offsets);

        const UInt8 * __restrict src = data.data();
        UInt8 * __restrict dst = res_data.data();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset next_offset = offsets[i];
            for (size_t j = prev_offset; j < next_offset; ++j)
                dst[j] = src[next_offset + prev_offset - j - 1];
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

        const UInt8 * __restrict src = data.data();
        UInt8 * __restrict dst = res_data.data();

        for (size_t i = 0; i < input_rows_count; ++i)
            for (size_t j = i * n; j < (i + 1) * n; ++j)
                dst[j] = src[(i * 2 + 1) * n - j - 1];
    }
};

}
