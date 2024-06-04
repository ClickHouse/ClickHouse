#pragma once

#include <Columns/ColumnString.h>

namespace DB
{

/** Reverse the string as a sequence of bytes.
  */
struct ReverseImpl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize_exact(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (size_t j = prev_offset; j < offsets[i] - 1; ++j)
                res_data[j] = data[offsets[i] + prev_offset - 2 - j];
            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t n, ColumnString::Chars & res_data)
    {
        res_data.resize_exact(data.size());
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            for (size_t j = i * n; j < (i + 1) * n; ++j)
                res_data[j] = data[(i * 2 + 1) * n - j - 1];
    }
};

}
