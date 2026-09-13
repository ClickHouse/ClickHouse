#pragma once

#include <Columns/ColumnString.h>
#include <base/unaligned.h>

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

    static void reverseBytes8(const UInt8 * src, UInt8 * dst)
    {
        const UInt64 value = std::byteswap(unalignedLoad<UInt64>(src));
        unalignedStore<UInt64>(dst, value);
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

            if (size == sizeof(UInt64))
                reverseBytes8(data.data() + prev_offset, res_data.data() + prev_offset);
            else if (size < 16)
            {
                for (size_t j = prev_offset; j < next_offset; ++j)
                    res_data[j] = data[next_offset + prev_offset - j - 1];
            }
            else
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

        if (n == sizeof(UInt64))
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t offset = i * n;
                reverseBytes8(data.data() + offset, res_data.data() + offset);
            }
            return;
        }

        if (n < 16)
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t offset = i * n;
                for (size_t j = offset; j < offset + n; ++j)
                    res_data[j] = data[offset * 2 + n - j - 1];
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
